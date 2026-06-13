//! Operator graph for streaming SQL execution.

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
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::sql_analysis::{
    apply_topk_filter, detect_asof_query, detect_processtime_join, detect_stream_join_query,
    detect_temporal_probe_query, detect_temporal_query, extract_table_references,
    StreamJoinDetection,
};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::{
    OrderOperatorConfig, TemporalJoinTranslatorConfig, WindowOperatorConfig,
};

#[async_trait]
pub(crate) trait GraphOperator: Send {
    /// `watermarks[i]` is the watermark for `inputs[i]`, derived from
    /// the upstream node's output watermark. Multi-input operators use
    /// per-input watermarks for independent eviction.
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError>;

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError>;
    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError>;

    fn estimated_state_bytes(&self) -> usize {
        0
    }

    /// Optional ceiling on this operator's output watermark. The graph holds the
    /// output watermark at `min(input_wm, hold)` so rows the operator has not yet
    /// emitted (e.g. async AI enrichment still in flight) are not treated as late
    /// by a downstream window. `None` = no hold. Default: no hold.
    fn watermark_hold(&self) -> Option<i64> {
        None
    }

    /// Whether the operator can accept new input this cycle. When `false`, the
    /// graph leaves this node's input buffered — so the source backpressures via
    /// the existing input-buffer gate — and still steps the operator with empty
    /// input so it can drain and emit. Default: always accept.
    fn wants_input(&self) -> bool {
        true
    }

    /// Fold a peer-shipped shuffle batch into operator state outside the normal
    /// `process` path. Checkpoint barrier alignment calls this for rows that
    /// arrived between cycles so they enter the snapshot. Default: no-op (for
    /// operators that don't consume the cross-node shuffle). `watermark` is the
    /// checkpoint watermark.
    #[cfg(feature = "cluster")]
    async fn ingest_shuffle(
        &mut self,
        _stage: &str,
        _batch: RecordBatch,
        _watermark: i64,
    ) -> Result<(), DbError> {
        Ok(())
    }

    /// Serialize this operator's state partitioned by vnode, for cross-node
    /// rehydration. Returns `Some(map)` from operators that key their state by
    /// a value the cluster shuffle routes (the aggregation fast-path); `None`
    /// (default) for operators that aren't vnode-partitionable — those recover
    /// from the whole-node manifest blob instead. The per-vnode bytes are the
    /// same encoding the operator's own restore/apply path consumes. A vnode
    /// demoted to the cold tier stages [`StagedSlice::Cold`] instead of
    /// bytes — staging nothing would read as "emptied" and drop the demoted
    /// state from recovery truth.
    #[cfg(feature = "cluster")]
    #[allow(clippy::disallowed_types)] // cold checkpoint path; vnode-keyed map
    fn checkpoint_by_vnode(
        &mut self,
        _vnode_count: u32,
    ) -> Result<
        Option<std::collections::HashMap<u32, crate::checkpoint_coordinator::StagedSlice>>,
        DbError,
    > {
        Ok(None)
    }

    /// Drop one vnode's state from memory after its bytes were confirmed
    /// written to the cold tier. Returns `false` to refuse — the vnode was
    /// touched since the last capture (the tier bytes would be stale) or
    /// the operator can't demote at all. Default: refuse.
    #[cfg(feature = "state-tier")]
    fn demote_vnode(&mut self, _vnode: u32, _vnode_count: u32) -> bool {
        false
    }

    /// Whether [`demote_vnode`](Self::demote_vnode) would succeed for `vnode`
    /// right now — the same eligibility guard, without dropping anything. The
    /// demotion pass checks this *before* writing the slice to the tier so a
    /// dirty vnode is skipped cheaply instead of written-then-rolled-back.
    /// Default: not demotable.
    #[cfg(feature = "state-tier")]
    fn can_demote(&self, _vnode: u32, _vnode_count: u32) -> bool {
        false
    }

    /// Merge one vnode's rehydrated state slice (produced by
    /// [`checkpoint_by_vnode`](Self::checkpoint_by_vnode) on whichever node
    /// last owned the vnode) into this operator. Default no-op for operators
    /// that don't partition by vnode.
    #[cfg(feature = "cluster")]
    fn apply_vnode_state(&mut self, _vnode: u32, _bytes: &[u8]) -> Result<(), DbError> {
        Ok(())
    }

    /// Wire the cold-tier request channel so the operator can fetch demoted
    /// vnode slices back into memory (promotion). Default no-op — only the
    /// vnode-sharded aggregate operator promotes. The same channel feeds the
    /// coordinator's forced-full re-uploads.
    #[cfg(feature = "state-tier")]
    fn attach_state_tier(&mut self, _tier: crate::state_tier::TierTx) {}

    /// Drain the vnodes this operator had demoted at the restored checkpoint
    /// (the cold tier is wiped on restart, so they must be replayed from
    /// their durable partials). Default: none. The restart path applies each
    /// vnode's slice via [`apply_vnode_state`](Self::apply_vnode_state).
    #[cfg(feature = "state-tier")]
    fn take_tier_cold_vnodes(&mut self) -> Vec<u32> {
        Vec::new()
    }
}

pub(crate) struct OperatorCheckpoint {
    pub data: Vec<u8>,
}

enum GateDecision {
    Run,
    Skip,
    Fail,
}

const STATS_SAMPLE_INTERVAL: u64 = 32;

/// `operators` uses std `HashMap` so rkyv's stock `Archive`/`Serialize`/
/// `Deserialize` impls apply (it supports `HashMap<K, V>` natively but
/// not `HashMap<K, V, FxHasher>`). Cold path — written once per
/// checkpoint interval, not on the hot query path. The `clippy::disallowed_types`
/// allow is scoped to the single file-level type alias below rather
/// than `#![allow]`-ing the whole module.
#[allow(clippy::disallowed_types)]
pub(crate) type OperatorStateMap = std::collections::HashMap<String, Vec<u8>>;

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

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
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

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
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
            .apply(&self.ctx, "pre-filter", batches)
            .await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }
}

pub(crate) struct OperatorGraph {
    nodes: Vec<GraphNode>,
    edges: Vec<GraphEdge>,
    topo_order: Vec<usize>,
    topo_dirty: bool,
    source_map: FxHashMap<Arc<str>, usize>,
    source_list: Vec<(Arc<str>, usize)>,
    /// Cached set of source node IDs for O(1) lookup in `execute_single_operator`.
    source_node_ids: FxHashSet<usize>,
    output_map: FxHashMap<Arc<str>, usize>,
    input_bufs: Vec<Vec<Vec<RecordBatch>>>,
    input_buf_bytes: Vec<Vec<usize>>,
    /// Per-node, per-input-port upstream node id. `input_sources[node][port] = upstream_node`.
    input_sources: Vec<Vec<usize>>,
    /// Per-node output watermark, set during `execute_cycle`.
    output_watermarks: Vec<i64>,
    max_input_buf_batches: usize,
    max_input_buf_bytes: Option<usize>,
    backpressure_policy: BackpressurePolicy,
    query_budget_ns: u64,
    /// Round-robin offset for deferred operator selection. Ensures fair
    /// scheduling when budget is continuously exceeded (not checkpointed).
    deferred_scan_offset: usize,
    stats_tick: u64,
    max_state_bytes: Option<usize>,
    ctx: SessionContext,
    prom: Option<Arc<EngineMetrics>>,
    lookup_registry: Option<Arc<laminar_sql::datafusion::LookupTableRegistry>>,
    source_schemas: FxHashMap<String, SchemaRef>,
    temporal_configs: Vec<TemporalJoinTranslatorConfig>,
    depends_on_stream: FxHashSet<usize>,
    order_configs: FxHashMap<usize, OrderOperatorConfig>,
    /// Per-table `LiveSourceHandle` for batch swapping without catalog churn.
    /// Covers both source tables (from `register_source_schema`) and
    /// intermediate tables (created lazily on first operator output).
    live_handles: FxHashMap<String, LiveSourceHandle>,
    /// Assembled AI subsystem (registry + providers + cache + call log).
    /// `None` unless `[ai]`/`[models]` are configured.
    ai_runtime: Option<Arc<crate::ai::AiRuntime>>,
    /// Main (multi-threaded) runtime handle for spawning Ring-1 workers (AI
    /// inference, lookup-enrich fetch) off the compute thread.
    main_runtime_handle: Option<tokio::runtime::Handle>,
    /// Partial (on-demand) lookup tables → their column names. Drives routing of
    /// lookup-enrich joins to the async operator and output-column disambiguation.
    partial_lookup_tables: FxHashMap<String, Vec<String>>,
    /// Plan-time AI routing errors collected during `add_query` (which returns
    /// `()`); surfaced by [`take_build_errors`](Self::take_build_errors) at start.
    build_errors: Vec<DbError>,
    /// Cluster-mode row-shuffle config for streaming aggregates.
    /// `None` outside cluster mode; threaded to `SqlQueryOperator` so
    /// pre-aggregate rows can be hash-routed to vnode owners.
    #[cfg(feature = "cluster")]
    cluster_shuffle: Option<crate::operator::sql_query::ClusterShuffleConfig>,
    /// Vnode count for per-vnode state capture and cold-tier demotion — the
    /// only piece of the shuffle topology that path needs (groups bucket by
    /// `key_hash % vnode_count`). Set from the shuffle registry in cluster
    /// mode, or from the vnode registry directly on a single node (no
    /// controller). `None` = no per-vnode capture.
    #[cfg(feature = "cluster")]
    vnode_count: Option<u32>,
    /// Shared handle to the DB's staged per-vnode rehydration map. Drained at
    /// the start of each cycle by [`apply_rehydrated_vnodes`](Self::apply_rehydrated_vnodes):
    /// vnodes this node newly acquired in a rebalance have their committed
    /// state merged into the matching operators before they process new rows.
    /// `None` outside cluster mode.
    #[cfg(feature = "cluster")]
    #[allow(clippy::disallowed_types)] // shares the DB's std-HashMap-typed handle
    rehydrated_vnode_state:
        Option<Arc<parking_lot::Mutex<std::collections::HashMap<u32, crate::db::RehydratedVnode>>>>,
    /// Cold-tier request channel, threaded to vnode-sharded aggregate
    /// operators for promotion of demoted slices. `None` until a state tier
    /// is wired; kept so operators hot-added by DDL also receive it.
    #[cfg(feature = "state-tier")]
    state_tier: Option<crate::state_tier::TierTx>,
}

impl OperatorGraph {
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            topo_order: Vec::new(),
            topo_dirty: true,
            source_map: FxHashMap::default(),
            source_list: Vec::new(),
            source_node_ids: FxHashSet::default(),
            output_map: FxHashMap::default(),
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
            max_state_bytes: None,
            #[cfg(feature = "cluster")]
            cluster_shuffle: None,
            #[cfg(feature = "cluster")]
            vnode_count: None,
            #[cfg(feature = "cluster")]
            rehydrated_vnode_state: None,
            #[cfg(feature = "state-tier")]
            state_tier: None,
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
            build_errors: Vec::new(),
        }
    }

    /// Install the assembled AI subsystem and the main runtime handle its
    /// inference workers spawn on. The handle MUST be the main multi-threaded
    /// runtime, never the compute runtime.
    pub fn set_ai_runtime(
        &mut self,
        runtime: Arc<crate::ai::AiRuntime>,
        handle: tokio::runtime::Handle,
    ) {
        self.ai_runtime = Some(runtime);
        self.main_runtime_handle = Some(handle);
    }

    /// Install the main runtime handle Ring-1 workers spawn on (independent of
    /// AI; the lookup-enrich operator needs it too). MUST be the main
    /// multi-threaded runtime, never the compute runtime.
    pub fn set_runtime_handle(&mut self, handle: tokio::runtime::Handle) {
        self.main_runtime_handle = Some(handle);
    }

    /// Register partial (on-demand) lookup tables and their column names so
    /// `add_query` can route lookup-enrich joins to the async operator.
    pub fn set_partial_lookup_tables(&mut self, tables: FxHashMap<String, Vec<String>>) {
        self.partial_lookup_tables = tables;
    }

    /// Take the first plan-time AI routing error collected during query
    /// construction, if any.
    ///
    /// # Errors
    ///
    /// Returns the first recorded [`DbError`] (e.g. unknown model, unsupported
    /// task, malformed AI query).
    pub fn take_build_errors(&mut self) -> Result<(), DbError> {
        match self.build_errors.drain(..).next() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub fn set_max_state_bytes(&mut self, limit: Option<usize>) {
        self.max_state_bytes = limit;
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
            ports.iter().any(|port| !port.is_empty())
                && !self.source_map.values().any(|&src| src == id)
        })
    }

    /// Estimated state bytes per operator, for the node-level memory budget
    /// and the per-operator gauge. Operators serve this from maintained
    /// counters or a throttled cache, so calling it once per budget probe is
    /// cheap.
    pub(crate) fn state_bytes_per_operator(&self) -> impl Iterator<Item = (&Arc<str>, usize)> {
        self.nodes
            .iter()
            .map(|n| (&n.name, n.operator.estimated_state_bytes()))
    }

    pub fn set_lookup_registry(
        &mut self,
        registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    ) {
        self.lookup_registry = Some(registry);
    }

    /// Install the row-shuffle config used by streaming aggregates in
    /// cluster mode.
    #[cfg(feature = "cluster")]
    pub fn set_cluster_shuffle(
        &mut self,
        config: crate::operator::sql_query::ClusterShuffleConfig,
    ) {
        self.vnode_count = Some(config.registry.vnode_count());
        self.cluster_shuffle = Some(config);
    }

    /// Install a vnode count for per-vnode capture and cold-tier demotion
    /// without a full shuffle config — the single-node tier path, where there
    /// is no controller or row transport but state still partitions by vnode.
    /// Must stay stable across restarts (demoted partials are keyed by vnode).
    #[cfg(feature = "state-tier")]
    pub(crate) fn set_vnode_count(&mut self, vnode_count: u32) {
        self.vnode_count = Some(vnode_count);
    }

    /// The vnode count used for per-vnode capture/demotion, if any.
    #[cfg(feature = "state-tier")]
    pub(crate) fn vnode_count(&self) -> Option<u32> {
        self.vnode_count
    }

    /// Wire the cold-tier request channel and hand it to every current
    /// operator (and, via the stored copy, every operator added later by
    /// DDL). Vnode-sharded aggregates use it for promotion; others ignore it.
    #[cfg(feature = "state-tier")]
    pub(crate) fn set_state_tier(&mut self, tier: crate::state_tier::TierTx) {
        for node in &mut self.nodes {
            node.operator.attach_state_tier(tier.clone());
        }
        self.state_tier = Some(tier);
    }

    /// The installed cluster row-shuffle config, if any; lets the pipeline
    /// callback reuse the shuffle sender to ship subscription output.
    #[cfg(feature = "cluster")]
    pub(crate) fn cluster_shuffle_config(
        &self,
    ) -> Option<&crate::operator::sql_query::ClusterShuffleConfig> {
        self.cluster_shuffle.as_ref()
    }

    /// Share the DB's staged per-vnode rehydration map so the graph can drain
    /// and apply rebalanced state into operators each cycle.
    #[cfg(feature = "cluster")]
    #[allow(clippy::disallowed_types)] // shares the DB's std-HashMap-typed handle
    pub fn set_rehydration_handle(
        &mut self,
        staged: Arc<parking_lot::Mutex<std::collections::HashMap<u32, crate::db::RehydratedVnode>>>,
    ) {
        self.rehydrated_vnode_state = Some(staged);
    }

    /// Drain the staged rehydration map for vnodes this node currently owns and
    /// merge each operator's committed slice into the matching live operator,
    /// then flip those vnodes `Restoring → Active`.
    ///
    /// Runs at the start of every [`execute_cycle`](Self::execute_cycle) so a
    /// newly-acquired vnode's state is in place before the operators process
    /// the cycle's rows. Best-effort: a slice that fails to decode or apply is
    /// logged and skipped, and the vnode still flips to `Active` (it serves
    /// from whatever state did apply, exactly like a vnode with no durable
    /// state). Cheap no-op when nothing is staged.
    #[cfg(feature = "cluster")]
    fn apply_rehydrated_vnodes(&mut self) {
        // Clone owned handles out so no borrow of `self` survives into the
        // `self.nodes.iter_mut()` dispatch below.
        let (registry, self_id, staged_arc) = match (
            self.cluster_shuffle.as_ref(),
            self.rehydrated_vnode_state.as_ref(),
        ) {
            (Some(cfg), Some(staged)) => {
                (Arc::clone(&cfg.registry), cfg.self_id, Arc::clone(staged))
            }
            _ => return,
        };

        // Drain only the staged vnodes we currently own (ownership may have
        // changed again since the snapshot was staged).
        let drained: Vec<(u32, crate::db::RehydratedVnode)> = {
            let mut guard = staged_arc.lock();
            if guard.is_empty() {
                return;
            }
            let owned: FxHashSet<u32> = laminar_core::state::owned_vnodes(&registry, self_id)
                .into_iter()
                .collect();
            let keys: Vec<u32> = guard
                .keys()
                .copied()
                .filter(|v| owned.contains(v))
                .collect();
            keys.into_iter()
                .filter_map(|v| guard.remove(&v).map(|r| (v, r)))
                .collect()
        };
        if drained.is_empty() {
            return;
        }

        for (vnode, rehydrated) in drained {
            match crate::vnode_partial::VnodePartial::decode(&rehydrated.bytes) {
                Ok(partial) => {
                    for (op_name, bytes) in &partial.operators {
                        if let Some(node) = self
                            .nodes
                            .iter_mut()
                            .find(|n| !n.removed && &*n.name == op_name.as_str())
                        {
                            if let Err(e) = node.operator.apply_vnode_state(vnode, bytes) {
                                tracing::warn!(
                                    operator = %op_name, vnode, error = %e,
                                    "failed to apply rehydrated vnode state"
                                );
                            }
                        } else {
                            tracing::debug!(
                                operator = %op_name, vnode,
                                "no live operator for rehydrated slice (topology drift)"
                            );
                        }
                    }
                    tracing::info!(
                        vnode,
                        epoch = rehydrated.epoch,
                        operators = partial.operators.len(),
                        "applied rehydrated vnode state"
                    );
                }
                Err(e) => tracing::warn!(
                    vnode, error = %e,
                    "rehydrated partial decode failed — vnode resumes from current state"
                ),
            }
            // The vnode is now serving regardless of apply outcome.
            registry.mark_active(&[vnode]);
        }
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
        let _ = self.ctx.deregister_table(name);
        if let Err(e) = self.ctx.register_table(name, Arc::new(provider)) {
            // This is a programming error — the table was just deregistered,
            // so registration should always succeed. Panic-worthy in debug,
            // but in release just log an error. The handle won't be stored,
            // so subsequent swap() calls will be no-ops for this table.
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
        self.temporal_configs.clone()
    }

    fn find_node(&self, name: &str) -> Option<usize> {
        self.nodes
            .iter()
            .position(|n| &*n.name == name && !n.removed)
    }

    fn ensure_source_node(&mut self, table_name: &str) -> usize {
        if let Some(&id) = self.source_map.get(table_name) {
            return id;
        }
        let node_id = self.nodes.len();
        let name: Arc<str> = Arc::from(table_name);
        self.nodes.push(GraphNode {
            name: Arc::clone(&name),
            operator: Box::new(SourcePassthrough),
            input_port_count: 1,
            output_routes: Vec::new(),
            removed: false,
        });
        self.input_bufs.push(vec![Vec::new()]);
        self.input_buf_bytes.push(vec![0]);
        self.input_sources.push(vec![usize::MAX]); // source nodes: no upstream
        self.output_watermarks.push(i64::MIN);
        self.source_map.insert(name, node_id);
        self.source_node_ids.insert(node_id);
        node_id
    }

    fn insert_filter_node(&mut self, name: &str, filter_sql: String, source_id: usize) -> usize {
        let node_id = self.nodes.len();
        self.nodes.push(GraphNode {
            name: Arc::from(name),
            operator: Box::new(SqlFilterOperator::new(filter_sql, self.ctx.clone(), name)),
            input_port_count: 1,
            output_routes: Vec::new(),
            removed: false,
        });
        self.input_bufs.push(vec![Vec::new()]);
        self.input_buf_bytes.push(vec![0]);
        self.input_sources.push(vec![usize::MAX]);
        self.output_watermarks.push(i64::MIN);
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

    /// Returns `true` if the query depends on another query (not just raw sources).
    #[allow(clippy::too_many_arguments)]
    fn wire_query_edges(
        &mut self,
        node_id: usize,
        temporal_probe_config: Option<&laminar_sql::translator::TemporalProbeConfig>,
        asof_config: Option<&laminar_sql::translator::AsofJoinTranslatorConfig>,
        stream_join_config: Option<&laminar_sql::translator::StreamJoinConfig>,
        stream_join_detection: Option<&StreamJoinDetection>,
        temporal_config: Option<&TemporalJoinTranslatorConfig>,
        table_refs: &FxHashSet<String>,
    ) -> bool {
        if let Some(tpc) = temporal_probe_config {
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
        idle_ttl_ms: Option<u64>,
        join_config: Option<Vec<laminar_sql::translator::JoinOperatorConfig>>,
    ) {
        use laminar_sql::translator::JoinOperatorConfig;

        // AI routing: a query with one `ai_*(...)` call becomes an
        // AiInferenceOperator over its source table, applying the residual
        // projection. Resolution/validation errors are recorded and surfaced
        // at start (this fn returns `()`).
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

        // Window-frame routing: a single-source query with a `… OVER (ORDER BY k
        // ROWS N PRECEDING)` frame (e.g. CORR) becomes a WindowFrameOperator that
        // retains bounded history and delegates the frame computation to
        // DataFusion. Detected off the SQL like the AI/join paths.
        if let Some(plan) = crate::sql_analysis::plan_frame_query(&sql) {
            self.build_frame_operator_node(&name, &plan);
            return;
        }

        // The planner's vec lets us short-circuit re-parsing for queries
        // that have no specialized join step (or no joins at all).
        // TemporalProbe is parsed off the token stream rather than the
        // sqlparser AST, so it is never present in `join_config` and
        // always needs its own detector pass.
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

        let (temporal_probe_config, temporal_probe_projection_sql) =
            detect_temporal_probe_query(&sql);
        let (asof_config, projection_sql) =
            if temporal_probe_config.is_none() && needs_specialized_detection {
                detect_asof_query(&sql)
            } else {
                (None, None)
            };
        let (temporal_config, temporal_projection_sql) =
            if temporal_probe_config.is_none() && needs_specialized_detection {
                detect_temporal_query(&sql)
            } else {
                (None, None)
            };
        let stream_join_detection =
            if temporal_probe_config.is_none() && needs_specialized_detection {
                // Interval join (time-bounded) first; else a plain equi-join as a
                // processing-time join. `create_operator` distinguishes them by
                // whether the config has time columns.
                detect_stream_join_query(&sql).or_else(|| detect_processtime_join(&sql))
            } else {
                None
            };
        let stream_join_config = stream_join_detection.as_ref().map(|d| d.config.clone());
        let stream_join_projection_sql = stream_join_detection
            .as_ref()
            .map(|d| d.projection_sql.clone());

        // Lookup-enrich: a partial/on-demand lookup join, hoisted to the
        // async-decoupled operator. Only when no other specialized join matched
        // and at least one partial lookup table is registered.
        let (lookup_enrich_config, lookup_projection_sql) = if temporal_probe_config.is_none()
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

        let projection_sql = projection_sql
            .or(temporal_probe_projection_sql)
            .or(temporal_projection_sql)
            .or(stream_join_projection_sql)
            .or(lookup_projection_sql);

        // Warn for undetected JOIN+BETWEEN
        if stream_join_config.is_none() && asof_config.is_none() && temporal_config.is_none() {
            let sql_upper = sql.to_uppercase();
            if sql_upper.contains("JOIN") && sql_upper.contains("BETWEEN") {
                tracing::warn!(
                    query = %name,
                    "Query contains JOIN with BETWEEN but was not detected as an interval join. \
                     It will execute as a batch join (matches within one cycle only). \
                     Ensure time columns in the BETWEEN clause are simple column references."
                );
            }
        }

        let mut table_refs = extract_table_references(&sql);
        // The lookup-enrich operator reads its lookup table from the registry,
        // not as a graph input — drop it so only the stream side is wired.
        if let Some(cfg) = &lookup_enrich_config {
            table_refs.remove(&cfg.table_name);
        }

        // Store temporal join config
        if let Some(ref tc) = temporal_config {
            self.temporal_configs.push(tc.clone());
        }

        // Create operator based on detection
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
            idle_ttl_ms,
        );

        // Determine input port count
        let input_port_count = if asof_config.is_some()
            || stream_join_config.is_some()
            || temporal_probe_config.is_some()
        {
            2
        } else {
            1
        };

        // Install the operator: replace a SourcePassthrough placeholder for this
        // name in place if a query referenced it before it was registered (so
        // downstream edges stay valid), else append a fresh node — see
        // `place_operator_node`. Source nodes are ensured first so their indices
        // are stable when wiring edges. The placeholder and fresh paths wire
        // identically, including `temporal_config`, so an out-of-order temporal
        // join is wired the same as one registered in order.
        self.ensure_query_source_nodes(
            temporal_probe_config.as_ref(),
            asof_config.as_ref(),
            stream_join_config.as_ref(),
            temporal_config.as_ref(),
            &table_refs,
        );
        let node_id = self.place_operator_node(name.as_str(), operator, input_port_count);
        let depends = self.wire_query_edges(
            node_id,
            temporal_probe_config.as_ref(),
            asof_config.as_ref(),
            stream_join_config.as_ref(),
            stream_join_detection.as_ref(),
            temporal_config.as_ref(),
            &table_refs,
        );
        if depends {
            self.depends_on_stream.insert(node_id);
        }
        if let Some(oc) = order_config {
            self.order_configs.insert(node_id, oc);
        }
        self.output_map.insert(Arc::from(name.as_str()), node_id);
        self.topo_dirty = true;
    }

    /// Install an operator for `name`, returning its node id. If a
    /// `SourcePassthrough` placeholder exists for the name (a downstream query
    /// referenced it before it was created), replace it in place so the
    /// placeholder's node id and outbound edges stay valid and downstream nodes
    /// receive this operator's output; otherwise append a fresh node. Callers
    /// must still ensure source nodes and wire edges around this. The caller
    /// owns ordering: ensure source nodes before, wire after.
    fn place_operator_node(
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
            // No longer a source: let its output watermark advance per run.
            self.source_node_ids.remove(&id);
            // Downstream nodes already wired to the placeholder now depend on a
            // real (possibly stream-fed) query.
            for &(target, _) in &self.nodes[id].output_routes {
                self.depends_on_stream.insert(target);
            }
            id
        } else {
            let id = self.nodes.len();
            self.nodes.push(GraphNode {
                name: Arc::from(name),
                operator,
                input_port_count,
                output_routes: Vec::new(),
                removed: false,
            });
            self.input_bufs.push(vec![Vec::new(); input_port_count]);
            self.input_buf_bytes.push(vec![0; input_port_count]);
            self.input_sources.push(vec![usize::MAX; input_port_count]);
            self.output_watermarks.push(i64::MIN);
            id
        }
    }

    /// Build an [`AiInferenceOperator`](crate::operator::ai_inference) for a
    /// planned AI query and wire it as a single-input node over the source table.
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

        // All runtime-dependent work happens here; the immutable `ai_runtime`
        // borrow ends before the `&mut self` node creation below.
        let (operator, table_refs): (Box<dyn GraphOperator>, FxHashSet<String>) = {
            let runtime = self.ai_runtime.as_ref().ok_or_else(|| {
                DbError::InvalidOperation(
                    "AI functions require `[ai]` providers and `[models]` configuration"
                        .to_string(),
                )
            })?;

            // Plan-time validation (task support + labels seam).
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

        // Wire as a single-input node reading the source table (no joins),
        // replacing a placeholder in place if a downstream query referenced this
        // name before it was created.
        self.ensure_query_source_nodes(None, None, None, None, &table_refs);
        let node_id = self.place_operator_node(name, operator, 1);
        let depends = self.wire_query_edges(node_id, None, None, None, None, None, &table_refs);
        if depends {
            self.depends_on_stream.insert(node_id);
        }
        self.output_map.insert(Arc::from(name), node_id);
        self.topo_dirty = true;
        Ok(())
    }

    /// Build a [`WindowFrameOperator`](crate::operator::window_frame) for a
    /// planned frame query and wire it as a single-input node over the source.
    fn build_frame_operator_node(
        &mut self,
        name: &str,
        plan: &crate::sql_analysis::FrameQueryPlan,
    ) {
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
        let node_id = self.place_operator_node(name, operator, 1);
        let depends = self.wire_query_edges(node_id, None, None, None, None, None, &table_refs);
        if depends {
            self.depends_on_stream.insert(node_id);
        }
        self.output_map.insert(Arc::from(name), node_id);
        self.topo_dirty = true;
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
        idle_ttl_ms: Option<u64>,
    ) -> Box<dyn GraphOperator> {
        use crate::operator;

        // On-demand lookup-enrich join → async-decoupled operator. Falls through
        // to the DataFusion lookup path if the registry/runtime handle is absent.
        if let Some(cfg) = lookup_enrich_config {
            if let (Some(reg), Some(handle)) = (&self.lookup_registry, &self.main_runtime_handle) {
                #[cfg_attr(not(feature = "cluster"), allow(unused_mut))]
                let mut op = operator::lookup_enrich::LookupEnrichOperator::new(
                    name,
                    cfg,
                    projection_sql.map(Arc::from),
                    self.ctx.clone(),
                    Arc::clone(reg),
                    handle.clone(),
                    self.prom.clone(),
                );
                // In cluster mode, key-shard the probe side across nodes for
                // cache affinity (same shuffle config the aggregate path uses).
                #[cfg(feature = "cluster")]
                if let Some(ref sc) = self.cluster_shuffle {
                    op.attach_cluster_shuffle(sc.clone());
                }
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
            // No time columns ⇒ plain equi-join → per-cycle batch join; with
            // time columns ⇒ interval join.
            if cfg.left_time_column.is_empty() && cfg.right_time_column.is_empty() {
                #[cfg_attr(not(feature = "cluster"), allow(unused_mut))]
                let mut op = operator::process_time_join::ProcessTimeJoinOperator::new(
                    name,
                    cfg.clone(),
                    projection_sql.map(Arc::from),
                    self.ctx.clone(),
                );
                #[cfg(feature = "cluster")]
                if let Some(ref sc) = self.cluster_shuffle {
                    op.attach_cluster_shuffle(sc.clone());
                }
                return Box::new(op);
            }
            #[cfg_attr(not(feature = "cluster"), allow(unused_mut))]
            let mut op = operator::interval_join::IntervalJoinOperator::new(
                name,
                cfg.clone(),
                projection_sql.map(Arc::from),
                self.ctx.clone(),
            );
            #[cfg(feature = "cluster")]
            if let Some(ref sc) = self.cluster_shuffle {
                op.attach_cluster_shuffle(sc.clone());
            }
            return Box::new(op);
        }

        // Non-windowed `now()` is only valid as the recognised retracting
        // temporal-filter shape under EMIT CHANGES; anything else gets a
        // typed LDB-1001 rejection rather than a silently frozen plan-time
        // `now()`. Windowed queries keep the existing path.
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

        let emit_changelog = emit_clause.is_some_and(|ec| matches!(ec, EmitClause::Changes));

        #[cfg_attr(
            not(any(feature = "cluster", feature = "state-tier")),
            allow(unused_mut)
        )]
        let mut op = operator::sql_query::SqlQueryOperator::new(
            name,
            sql,
            self.ctx.clone(),
            self.prom.clone(),
            emit_changelog,
            idle_ttl_ms,
        );
        #[cfg(feature = "cluster")]
        if let Some(ref cfg) = self.cluster_shuffle {
            op.attach_cluster_shuffle(cfg.clone());
        }
        #[cfg(feature = "state-tier")]
        if let Some(tier) = self.state_tier.clone() {
            op.attach_state_tier(tier);
            // Cold-tier promotion needs the vnode count to detect rows landing
            // on demoted vnodes (single-node has no shuffle config to read it
            // from, so it is threaded separately).
            if let Some(vnode_count) = self.vnode_count {
                op.set_vnode_count(vnode_count);
            }
        }
        Box::new(op)
    }

    pub fn remove_query(&mut self, name: &str) {
        let Some(node_id) = self.find_node(name) else {
            return;
        };

        // Collect IDs of this node + any child prefilter nodes (name prefix match).
        let prefix = format!("{name}::");
        let ids_to_remove: smallvec::SmallVec<[usize; 3]> = std::iter::once(node_id)
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
        }

        // Clean dangling output routes pointing to any removed node
        for node in &mut self.nodes {
            node.output_routes
                .retain(|&(t, _)| !ids_to_remove.contains(&t));
        }

        self.output_map.remove(name);
        self.topo_dirty = true;
    }

    fn compute_topo_order(&mut self) {
        // Kahn's algorithm
        let n = self.nodes.len();
        let mut in_degree = vec![0usize; n];
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n];

        for edge in &self.edges {
            if !self.nodes[edge.source].removed && !self.nodes[edge.target].removed {
                in_degree[edge.target] += 1;
                dependents[edge.source].push(edge.target);
            }
        }

        // Deduplicate dependents
        for deps in &mut dependents {
            deps.sort_unstable();
            deps.dedup();
        }

        // Recalculate in_degree from deduped
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

        // Fallback: if cycle detected, append missing non-removed nodes
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

        self.source_list.clear();
        self.source_list
            .extend(self.source_map.iter().map(|(k, v)| (Arc::clone(k), *v)));

        self.topo_dirty = false;
    }

    fn register_source_tables(&mut self, source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        for (name, batches) in source_batches {
            if batches.is_empty() {
                continue;
            }
            // Lazily create the provider if register_source_schema wasn't called
            // (e.g., tests that skip the lifecycle).
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

    #[allow(clippy::too_many_lines)]
    async fn execute_single_operator(
        &mut self,
        node_id: usize,
        current_watermark: i64,
        results: &mut FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<(), DbError> {
        // If the operator is backpressured (wants_input == false), leave its
        // input buffered so the upstream gate throttles the source, and step it
        // with empty input so it can still drain and emit.
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

        // Source nodes (input_sources = [usize::MAX]) have output_watermarks
        // pre-seeded from per-source watermarks in execute_cycle. Don't overwrite.
        if !self.source_node_ids.contains(&node_id) {
            let mut wm = watermarks
                .iter()
                .copied()
                .min()
                .unwrap_or(current_watermark);
            // Hold the watermark behind any rows the operator has buffered but
            // not yet emitted (e.g. in-flight AI enrichment), so a downstream
            // window doesn't close past them.
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

        let batches = match output_result {
            Ok(b) => {
                // When backpressured (!accept) the input was never taken, so
                // leave the buffer intact. Otherwise reuse the outer Vecs and
                // their capacities; the operator borrowed inputs, so the
                // RecordBatches are still in place. clear() preserves capacity.
                if accept {
                    for v in &mut inputs {
                        v.clear();
                    }
                    input_bytes.fill(0);
                    self.input_bufs[node_id] = inputs;
                    self.input_buf_bytes[node_id] = input_bytes;
                }
                b
            }
            Err(e) => {
                if accept && self.depends_on_stream.contains(&node_id) {
                    // Put batches back so they're retried next cycle.
                    self.input_bufs[node_id] = inputs;
                    self.input_buf_bytes[node_id] = input_bytes;
                    tracing::debug!(
                        query = %self.nodes[node_id].name,
                        error = %e,
                        "Query deferred (upstream not ready); batches preserved for retry"
                    );
                    return Ok(());
                }
                if !accept {
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

        if let Some(limit) = self.max_state_bytes {
            let size = self.nodes[node_id].operator.estimated_state_bytes();
            if size >= limit {
                return Err(DbError::Pipeline(format!(
                    "state size limit exceeded for query '{}' ({size} bytes >= {limit} limit)",
                    self.nodes[node_id].name
                )));
            }
            if size >= limit * 4 / 5 {
                tracing::warn!(
                    query = %self.nodes[node_id].name,
                    size_bytes = size,
                    limit_bytes = limit,
                    "state size at 80% of limit"
                );
            }
        }

        let batches = if let Some(oc) = self.order_configs.get(&node_id) {
            match oc {
                OrderOperatorConfig::TopK(c) => apply_topk_filter(&batches, c.k),
                OrderOperatorConfig::PerGroupTopK(c) => apply_topk_filter(&batches, c.k),
                _ => batches,
            }
        } else {
            batches
        };

        if !batches.is_empty() {
            let node_name = Arc::clone(&self.nodes[node_id].name);
            let has_routes = !self.nodes[node_id].output_routes.is_empty();
            let is_output = self.output_map.values().any(|&id| id == node_id);

            // Register intermediate for downstream SQL queries (catalog lookup).
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

            // Collect output for the caller.
            if is_output {
                results.insert(node_name, batches.clone());
            }

            let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
            let route_count = self.nodes[node_id].output_routes.len();
            if route_count == 1 {
                let (target, port) = self.nodes[node_id].output_routes[0];
                self.push_to_port(target, port, batches, bytes);
            } else if route_count > 1 {
                // Clone batches N-1 times (one for each non-final route);
                // the final route takes ownership of the original. Reading
                // routes via an indexed loop avoids cloning output_routes
                // just to iterate it.
                for i in 0..route_count - 1 {
                    let (target, port) = self.nodes[node_id].output_routes[i];
                    self.push_to_port(target, port, batches.clone(), bytes);
                }
                let (target, port) = self.nodes[node_id].output_routes[route_count - 1];
                self.push_to_port(target, port, batches, bytes);
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    pub async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
        source_watermarks: Option<&FxHashMap<Arc<str>, i64>>,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, DbError> {
        // Merge any rebalanced-in vnode state into operators before they see
        // this cycle's rows, so the snapshot a new owner resumes from is in
        // place first.
        #[cfg(feature = "cluster")]
        self.apply_rehydrated_vnodes();

        if self.topo_dirty {
            self.compute_topo_order();
        }

        self.register_source_tables(source_batches);

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

        let mut results = FxHashMap::default();
        let cycle_start = std::time::Instant::now();
        let topo_len = self.topo_order.len();

        for i in 0..topo_len {
            let node_id = self.topo_order[i];

            if self.nodes[node_id].removed {
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

            // Budget check
            if i > 0 {
                #[allow(clippy::cast_possible_truncation)]
                let elapsed_ns = cycle_start.elapsed().as_nanos() as u64;
                if elapsed_ns > self.query_budget_ns {
                    tracing::debug!(
                        skipped = topo_len - i,
                        elapsed_ms = elapsed_ns / 1_000_000,
                        "per-query budget exceeded — deferring remaining operators"
                    );

                    // Forward progress: run one deferred operator to prevent
                    // tail-operator starvation. Round-robin ensures fair
                    // scheduling under continuous input.
                    let deferred_count = topo_len - i;
                    let start = self.deferred_scan_offset % deferred_count;
                    for offset in 0..deferred_count {
                        let j = i + (start + offset) % deferred_count;
                        let deferred_id = self.topo_order[j];
                        if self.nodes[deferred_id].removed {
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
                                self.finish_cycle();
                                return Err(DbError::BackpressureFail(format!(
                                    "input buffer at capacity downstream of '{}'",
                                    self.nodes[deferred_id].name
                                )));
                            }
                            GateDecision::Run => {}
                        }
                        if let Err(e) = self
                            .execute_single_operator(deferred_id, current_watermark, &mut results)
                            .await
                        {
                            self.finish_cycle();
                            return Err(e);
                        }
                        self.deferred_scan_offset = self.deferred_scan_offset.wrapping_add(1);
                        break; // Only one per cycle
                    }

                    break;
                }
            }

            if let Err(e) = self
                .execute_single_operator(node_id, current_watermark, &mut results)
                .await
            {
                self.finish_cycle();
                return Err(e);
            }
        }

        self.finish_cycle();

        #[cfg(debug_assertions)]
        self.debug_assert_byte_sums();

        self.stats_tick = self.stats_tick.wrapping_add(1);
        if self.stats_tick.is_multiple_of(STATS_SAMPLE_INTERVAL) {
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

        Ok(results)
    }

    /// Route one peer-shipped shuffle batch to the operator named `stage`.
    /// Unknown stage (no such live operator) is dropped.
    #[cfg(feature = "cluster")]
    async fn ingest_to_stage(
        &mut self,
        stage: &str,
        batch: RecordBatch,
        watermark: i64,
    ) -> Result<(), DbError> {
        let node_name = stage
            .strip_suffix("::left")
            .or_else(|| stage.strip_suffix("::right"))
            .unwrap_or(stage);
        if let Some(idx) = self.find_node(node_name) {
            self.nodes[idx]
                .operator
                .ingest_shuffle(stage, batch, watermark)
                .await?;
        }
        Ok(())
    }

    /// Chandy–Lamport alignment of the cross-node shuffle, run just before
    /// `snapshot_state` so the snapshot captures every pre-checkpoint row a peer
    /// shipped. Fans out `Barrier(checkpoint_id)` to peers, then drains the
    /// receiver — folding each `VnodeData` into its operator via `ingest_shuffle`
    /// — until every peer's barrier is observed. No-op without a cross-node
    /// shuffle. See `docs/plans/cross-node-shuffle-barrier-alignment.md`.
    ///
    /// Relies on **full-membership commit** (`wait_for_quorum` requires every
    /// live follower): no peer resumes the next epoch until all have aligned, so
    /// no next-epoch frame can arrive mid-alignment. A move to majority quorum
    /// would reintroduce that case and need per-peer post-barrier buffering.
    ///
    /// # Errors
    /// Fails the checkpoint (caller retries) on timeout or a closed receiver —
    /// the deliberate degradation under a straggling peer.
    #[cfg(feature = "cluster")]
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn align_shuffle_barriers(
        &mut self,
        checkpoint_id: u64,
        watermark: i64,
        live: &[u64],
        controller: Option<&laminar_core::cluster::control::ClusterController>,
    ) -> Result<(), DbError> {
        use laminar_core::checkpoint::barrier::CheckpointBarrier;
        use laminar_core::cluster::control::Phase;
        use laminar_core::shuffle::{BarrierTracker, ShuffleMessage};

        const ALIGN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

        let Some(cfg) = self.cluster_shuffle.clone() else {
            return Ok(());
        };
        // Fan-out set: nodes we ship rows to (other vnode owners). Wait set:
        // nodes that ship to us — the live producers, but only if we own a vnode
        // (a node owning none, e.g. drained, receives nothing yet still ships, so
        // the two sets differ under skewed assignment). Producers = live
        // membership, not the owners.
        let output_peers: Vec<u64> = laminar_core::state::peer_owners(&cfg.registry, cfg.self_id)
            .iter()
            .map(|n| n.0)
            .collect();
        let input_peers: Vec<u64> =
            if laminar_core::state::owned_vnodes(&cfg.registry, cfg.self_id).is_empty() {
                Vec::new()
            } else {
                live.iter()
                    .copied()
                    .filter(|&id| id != cfg.self_id.0)
                    .collect()
            };
        if output_peers.is_empty() && input_peers.is_empty() {
            return Ok(()); // not part of any cross-node shuffle
        }

        // Rows already pulled into the receiver's per-stage holdover are
        // pre-barrier (the per-cycle drain carries no barriers).
        for (stage, batch) in cfg.receiver.drain_all_staged() {
            self.ingest_to_stage(&stage, batch, watermark).await?;
        }

        let barrier = CheckpointBarrier::new(checkpoint_id, 0);
        if !output_peers.is_empty() {
            cfg.sender
                .fan_out_barrier(&output_peers, barrier)
                .await
                .map_err(|e| DbError::Pipeline(format!("shuffle barrier fan-out: {e}")))?;
        }
        if input_peers.is_empty() {
            return Ok(()); // we receive from no peer — nothing to align on
        }

        tracing::info!(
            checkpoint_id,
            self_id = cfg.self_id.0,
            output_peers = ?output_peers,
            input_peers = ?input_peers,
            "align_shuffle_barriers: starting alignment"
        );

        let tracker = BarrierTracker::new(input_peers.len() + 1);
        let peer_port: FxHashMap<u64, usize> = input_peers
            .iter()
            .enumerate()
            .map(|(i, &p)| (p, i + 1))
            .collect();
        tracker.observe(0, barrier); // our own input

        // A peer that fanned its barrier out before we entered alignment had it
        // stashed by the per-cycle drain (which would otherwise drop it) — observe
        // those before blocking on the live wire.
        for (from, b) in cfg.receiver.drain_staged_barriers() {
            if b.checkpoint_id == checkpoint_id {
                if let Some(&port) = peer_port.get(&from) {
                    tracing::info!(
                        checkpoint_id,
                        from_peer = from,
                        "align_shuffle_barriers: observed pre-staged peer barrier"
                    );
                    if tracker.observe(port, b).is_some() {
                        tracing::info!(
                            checkpoint_id,
                            "align_shuffle_barriers: all peers aligned (pre-staged)"
                        );
                        return Ok(()); // all peers already aligned
                    }
                }
            }
        }

        let alignment_timeout = tokio::time::sleep(ALIGN_TIMEOUT);
        tokio::pin!(alignment_timeout);
        let mut check_interval = tokio::time::interval(std::time::Duration::from_millis(500));
        loop {
            tokio::select! {
                res = cfg.receiver.recv() => {
                    match res {
                        None => {
                            return Err(DbError::Pipeline(
                                "shuffle receiver closed during barrier alignment".into(),
                            ));
                        }
                        Some((from, ShuffleMessage::Barrier(b))) if b.checkpoint_id == checkpoint_id => {
                            tracing::info!(
                                checkpoint_id,
                                from_peer = from,
                                "align_shuffle_barriers: received peer barrier"
                            );
                            if let Some(&port) = peer_port.get(&from) {
                                if tracker.observe(port, b).is_some() {
                                    tracing::info!(
                                        checkpoint_id,
                                        "align_shuffle_barriers: all peers aligned successfully"
                                    );
                                    break;
                                }
                            }
                        }
                        Some((_, ShuffleMessage::VnodeData(stage, _vnode, batch))) => {
                            self.ingest_to_stage(&stage, batch, watermark).await?;
                        }
                        Some(_) => {}
                    }
                }
                _ = check_interval.tick() => {
                    if let Some(ctrl) = controller {
                        if let Ok(Some(ann)) = ctrl.observe_barrier().await {
                            if ann.checkpoint_id == checkpoint_id && ann.phase == Phase::Abort {
                                return Err(DbError::Pipeline(format!(
                                    "checkpoint {checkpoint_id} was aborted by leader"
                                )));
                            }
                            // Observation is latest-wins, so this checkpoint's
                            // Abort can be superseded before we ever see it. A
                            // NEWER announcement is just as conclusive: ids are
                            // never reused (failed epochs are abandoned), and
                            // the leader only moves on once this checkpoint is
                            // aligned cluster-wide or dead — either way the
                            // peer barriers we are waiting for will never
                            // arrive. Without this release, a rejoining node
                            // livelocks one epoch behind the cluster, timing
                            // out every alignment.
                            if ann.checkpoint_id > checkpoint_id {
                                return Err(DbError::Pipeline(format!(
                                    "checkpoint {checkpoint_id} superseded by {} — alignment abandoned",
                                    ann.checkpoint_id
                                )));
                            }
                        }
                    }
                }
                () = &mut alignment_timeout => {
                    return Err(DbError::Pipeline(format!(
                        "shuffle barrier alignment timed out for checkpoint {checkpoint_id}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Append a node with a given operator (test-only, for exercising
    /// `align_shuffle_barriers` without the full query-build path).
    #[cfg(all(test, feature = "cluster"))]
    pub(crate) fn push_test_node(&mut self, name: &str, operator: Box<dyn GraphOperator>) {
        self.nodes.push(GraphNode {
            name: Arc::from(name),
            operator,
            input_port_count: 1,
            output_routes: Vec::new(),
            removed: false,
        });
        self.input_bufs.push(vec![Vec::new()]);
        self.input_buf_bytes.push(vec![0]);
        self.input_sources.push(vec![usize::MAX]);
        self.output_watermarks.push(i64::MIN);
        self.topo_dirty = true;
    }

    // &mut self: some accumulators need &mut for state()
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
            version: 1,
            operators,
        }))
    }

    /// Per-vnode operator-state snapshot for cross-node rehydration.
    ///
    /// Returns `vnode → (operator_name → vnode-slice bytes)` for every operator
    /// that opts into per-vnode checkpointing (see
    /// [`GraphOperator::checkpoint_by_vnode`]). Empty when no vnode topology is
    /// installed. The count comes from the shuffle registry in cluster mode, or
    /// the single-node tier count — the same partition the routing (or, single
    /// node, all-local ownership) delivered each key under.
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

    /// Ask the named operator to drop one vnode's state after its slice
    /// was confirmed in the cold tier. `false` = refused (the vnode was
    /// touched since the last capture, or the operator can't demote).
    #[cfg(feature = "state-tier")]
    pub(crate) fn demote_vnode(&mut self, operator: &str, vnode: u32) -> bool {
        let Some(vnode_count) = self.vnode_count else {
            return false;
        };
        let demoted = self
            .nodes
            .iter_mut()
            .find(|n| !n.removed && &*n.name == operator)
            .is_some_and(|n| n.operator.demote_vnode(vnode, vnode_count));
        // Count only effective demotions (the slice actually left memory), so
        // the metric is not inflated by tier writes that get rolled back.
        if demoted {
            if let Some(ref prom) = self.prom {
                prom.state_tier_demote_total.inc();
            }
        }
        demoted
    }

    /// Whether the named operator could demote `vnode` right now (clean since
    /// its last capture). Lets the demotion pass skip dirty candidates before
    /// any tier I/O. `false` when there is no vnode topology or no such
    /// operator.
    #[cfg(feature = "state-tier")]
    pub(crate) fn can_demote(&self, operator: &str, vnode: u32) -> bool {
        let Some(vnode_count) = self.vnode_count else {
            return false;
        };
        self.nodes
            .iter()
            .find(|n| !n.removed && &*n.name == operator)
            .is_some_and(|n| n.operator.can_demote(vnode, vnode_count))
    }

    /// After a restart restore, the vnodes each operator had demoted to the
    /// cold tier — their groups are absent from the manifest blob and must be
    /// replayed from their durable partials. `(operator_name, cold_vnodes)`.
    #[cfg(feature = "state-tier")]
    pub(crate) fn take_tier_cold_vnodes(&mut self) -> Vec<(String, Vec<u32>)> {
        let mut out = Vec::new();
        for node in &mut self.nodes {
            if node.removed {
                continue;
            }
            let cold = node.operator.take_tier_cold_vnodes();
            if !cold.is_empty() {
                out.push((node.name.to_string(), cold));
            }
        }
        out
    }

    /// Apply one operator's slice of one vnode's partial (restart cold-vnode
    /// rehydration). Targets a single operator by name so a vnode partial,
    /// which bundles every operator's slice, doesn't double-apply operators
    /// already recovered from the manifest.
    #[cfg(feature = "state-tier")]
    pub(crate) fn apply_vnode_slice(
        &mut self,
        operator: &str,
        vnode: u32,
        bytes: &[u8],
    ) -> Result<(), DbError> {
        match self
            .nodes
            .iter_mut()
            .find(|n| !n.removed && &*n.name == operator)
        {
            Some(node) => node.operator.apply_vnode_state(vnode, bytes),
            None => Ok(()),
        }
    }

    pub fn restore_state(&mut self, checkpoint: &GraphCheckpoint) -> Result<usize, DbError> {
        let mut restored = 0;
        for node in &mut self.nodes {
            if node.removed {
                continue;
            }
            if let Some(bytes) = checkpoint.operators.get(&*node.name) {
                node.operator.restore(OperatorCheckpoint {
                    data: bytes.clone(),
                })?;
                restored += 1;
            }
        }
        Ok(restored)
    }

    pub fn serialize_checkpoint(cp: &GraphCheckpoint) -> Result<Vec<u8>, DbError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(cp)
            .map(|v| v.to_vec())
            .map_err(|e| DbError::Pipeline(format!("operator graph checkpoint serialization: {e}")))
    }

    pub fn restore_from_bytes(&mut self, bytes: &[u8]) -> Result<usize, DbError> {
        let checkpoint: GraphCheckpoint =
            rkyv::from_bytes::<GraphCheckpoint, rkyv::rancor::Error>(bytes).map_err(|e| {
                DbError::Pipeline(format!("operator graph checkpoint deserialization: {e}"))
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

    /// Records the batches handed to `ingest_shuffle`.
    #[cfg(feature = "cluster")]
    struct RecordingOperator(Arc<parking_lot::Mutex<Vec<RecordBatch>>>);

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
        async fn ingest_shuffle(
            &mut self,
            _stage: &str,
            batch: RecordBatch,
            _watermark: i64,
        ) -> Result<(), DbError> {
            self.0.lock().push(batch);
            Ok(())
        }
    }

    /// A peer ships a row + its barrier; alignment folds the row into the target
    /// operator (so it would enter the snapshot) and completes once the peer's
    /// barrier is observed.
    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn align_shuffle_barriers_folds_peer_rows_then_aligns() {
        use laminar_core::checkpoint::barrier::CheckpointBarrier;
        use laminar_core::shuffle::{ShuffleMessage, ShuffleReceiver, ShuffleSender};
        use laminar_core::state::{NodeId, VnodeRegistry};

        // 2 vnodes: node 1 owns vnode 0, node 2 owns vnode 1 (so node 1's peer = {2}).
        let registry = Arc::new(VnodeRegistry::new(2));
        registry.set_assignment(vec![NodeId(1), NodeId(2)].into());

        let recv1 = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        let recv2 = Arc::new(
            ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        let send1 = ShuffleSender::new(1);
        send1.register_peer(2, recv2.local_addr()).await;
        let send2 = ShuffleSender::new(2);
        send2.register_peer(1, recv1.local_addr()).await;

        let recorded = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
        graph.push_test_node("out", Box::new(RecordingOperator(Arc::clone(&recorded))));
        graph.set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
            registry,
            sender: Arc::new(send1),
            receiver: Arc::clone(&recv1),
            self_id: NodeId(1),
        });

        // Peer (node 2) ships a row for stage "out", then its barrier for cp 7.
        let batch = test_batch();
        send2
            .send_to(
                1,
                &ShuffleMessage::VnodeData("out".into(), 0, batch.clone()),
            )
            .await
            .unwrap();
        send2
            .send_to(1, &ShuffleMessage::Barrier(CheckpointBarrier::new(7, 0)))
            .await
            .unwrap();

        graph
            .align_shuffle_barriers(7, 0, &[1, 2], None)
            .await
            .unwrap();

        // Node 1 must have fanned its own barrier out to node 2.
        let (from, msg) = recv2.recv().await.unwrap();
        assert_eq!(from, 1);
        assert!(matches!(msg, ShuffleMessage::Barrier(b) if b.checkpoint_id == 7));

        let got = recorded.lock();
        assert_eq!(
            got.len(),
            1,
            "peer's pre-barrier row folded into the operator"
        );
        assert_eq!(got[0].num_rows(), batch.num_rows());
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
            None,
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
            None,
        );
        graph.add_query(
            "q2".to_string(),
            "SELECT symbol FROM q1 WHERE price > 100".to_string(),
            None,
            None,
            None,
            None,
            None,
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
            None,
        );
        graph.add_query(
            "q1".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            None,
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
            None,
        );
        assert!(graph.output_map.contains_key("q1"));

        graph.remove_query("q1");
        assert!(!graph.output_map.contains_key("q1"));
        assert!(graph.nodes[1].removed); // node 0 = source, node 1 = q1
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
            None,
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

    // --- AI routing (B5.3b) ---

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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
        );
        graph.add_query(
            "q2".to_string(),
            "SELECT symbol FROM trades".to_string(),
            None,
            None,
            None,
            None,
            None,
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
            None,
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
            None,
        );
        let mut src = FxHashMap::default();
        src.insert(Arc::from("trades"), vec![test_batch()]);
        // Frontier 5000ms: both rows are members (exit 11000/12000) ⇒ +1,+1.
        let r = g1.execute_cycle(&src, 5_000, None).await.unwrap();
        assert_eq!(total_rows(&r, "recent"), 2);

        // Snapshot + restore through the real GraphCheckpoint/rkyv path.
        let cp = g1.snapshot_state().unwrap().expect("buffered state");
        let bytes = OperatorGraph::serialize_checkpoint(&cp).unwrap();
        let mut g2 = test_graph();
        g2.add_query(
            "recent".into(),
            sql.into(),
            Some(EmitClause::Changes),
            None,
            None,
            None,
            None,
        );
        assert_eq!(g2.restore_from_bytes(&bytes).unwrap(), 1);

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
            None,
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
            None,
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
            None,
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
            None,
        );
        graph.add_query(
            "step2".to_string(),
            "SELECT symbol, doubled FROM step1 WHERE doubled > 400".to_string(),
            None,
            None,
            None,
            None,
            None,
        );

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        // step1: AAPL=300, GOOG=5600 (2 rows)
        assert_eq!(total_rows(&r, "step1"), 2);
        // step2: only GOOG=5600 passes WHERE doubled > 400
        assert_eq!(total_rows(&r, "step2"), 1);
    }

    #[tokio::test]
    async fn test_og_diamond_dag() {
        // source → A, source → B, A+B → C  (diamond fan-out/fan-in)
        let mut graph = test_graph();
        graph.add_query(
            "high".to_string(),
            "SELECT symbol, price FROM trades WHERE price > 200".to_string(),
            None,
            None,
            None,
            None,
            None,
        );
        graph.add_query(
            "low".to_string(),
            "SELECT symbol, price FROM trades WHERE price <= 200".to_string(),
            None,
            None,
            None,
            None,
            None,
        );
        // C selects from both A and B — this will use cached plan (multi-source)
        graph.add_query(
            "combined".to_string(),
            "SELECT h.symbol, h.price FROM high h INNER JOIN low l ON h.symbol = l.symbol"
                .to_string(),
            None,
            None,
            None,
            None,
            None,
        );

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        assert_eq!(total_rows(&r, "high"), 1); // GOOG
        assert_eq!(total_rows(&r, "low"), 1); // AAPL
                                              // combined: inner join on symbol — no shared symbols, so empty
        assert_eq!(total_rows(&r, "combined"), 0);
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
            None,
        );
        graph.add_query(
            "q2".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            None,
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
                None,
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
    async fn test_og_state_size_limit() {
        // Set a very low state size limit — aggregate state should exceed it
        let mut graph = test_graph();
        graph.set_max_state_bytes(Some(1)); // 1 byte limit

        graph.add_query(
            "agg".to_string(),
            "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
            None,
            None,
            None,
            None,
            None,
        );

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        let result = graph.execute_cycle(&source, i64::MAX, None).await;
        assert!(result.is_err(), "state size limit should be exceeded");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("state size limit exceeded"),
            "unexpected error: {err_msg}"
        );
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
            None,
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
        let bytes = OperatorGraph::serialize_checkpoint(&cp).unwrap();

        // Create a new graph with same query and restore
        let mut graph2 = test_graph();
        graph2.add_query(
            "agg".to_string(),
            "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
            None,
            None,
            None,
            None,
            None,
        );

        // Need one cycle to lazy-init state before restore will take effect
        let _ = graph2.execute_cycle(&source, i64::MAX, None).await.unwrap();
        let restored = graph2.restore_from_bytes(&bytes).unwrap();
        assert!(restored > 0, "should restore at least one operator");

        // Next cycle should show accumulated state from both the initial
        // cycle in graph2 plus the restored state from graph1
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
            None,
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
            None,
        );
        graph.add_query(
            "q1".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
            None,
            None,
            None,
            None,
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
        );
        graph.add_query(
            "downstream".to_string(),
            "SELECT symbol FROM proj".to_string(),
            None,
            None,
            None,
            None,
            None,
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
            None,
        );

        // Now register `derived` — this replaces the placeholder.
        graph.add_query(
            "derived".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
            None,
            None,
            None,
            None,
            None,
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
            None,
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

        graph.add_query(
            "enriched".to_string(),
            "SELECT t.symbol, t.price, i.company_name \
             FROM trades t LEFT JOIN instruments i ON t.symbol = i.symbol"
                .to_string(),
            None,
            None,
            None,
            None,
            None,
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
            None,
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
        // when buffered left (type=A) rows see right (type=B) rows.
        let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();

        source.clear();
        source.insert(Arc::from("events"), vec![batch]);
        let results = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();

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
            None,
        );
        graph.add_query(
            "consumer".to_string(),
            "SELECT symbol FROM producer".to_string(),
            None,
            None,
            None,
            None,
            None,
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
            None,
        );
        graph.add_query(
            "consumer".to_string(),
            "SELECT symbol FROM producer".to_string(),
            None,
            None,
            None,
            None,
            None,
        );
        let consumer_id = *graph.output_map.get("consumer").unwrap();
        prefill_port(&mut graph, consumer_id, 0, vec![test_batch()]);

        let producer_id = *graph.output_map.get("producer").unwrap();
        assert!(graph.is_downstream_at_capacity(producer_id));
    }
}
