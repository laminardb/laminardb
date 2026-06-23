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
    /// `watermarks[i]` is the upstream output watermark for `inputs[i]`.
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

    /// Output watermark ceiling. Holds the watermark at `min(input_wm, hold)` so
    /// in-flight rows (e.g. async AI enrichment) aren't treated as late downstream.
    /// `None` = no hold.
    fn watermark_hold(&self) -> Option<i64> {
        None
    }

    /// Whether the operator can accept new input this cycle. When `false`, input
    /// stays buffered and the operator is still stepped with empty input to drain.
    fn wants_input(&self) -> bool {
        true
    }

    /// Fold a peer-shipped shuffle batch into state outside the normal `process`
    /// path so barrier-aligned rows enter the snapshot.
    #[cfg(feature = "cluster")]
    async fn ingest_shuffle(
        &mut self,
        _stage: &str,
        _batch: RecordBatch,
        _watermark: i64,
    ) -> Result<(), DbError> {
        Ok(())
    }

    /// Per-vnode state snapshot for cross-node rehydration. `None` for operators
    /// that don't key state by vnode (they recover from the whole-node manifest).
    /// A cold-tier vnode stages [`StagedSlice::Cold`] rather than bytes so it
    /// isn't treated as emptied by recovery.
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

    /// Drop one vnode's in-memory state after the cold-tier write is confirmed.
    /// Returns `false` if the vnode was modified since the last capture.
    #[cfg(feature = "state-tier")]
    fn demote_vnode(&mut self, _vnode: u32, _vnode_count: u32) -> bool {
        false
    }

    /// Whether [`demote_vnode`](Self::demote_vnode) would succeed right now.
    /// Checked before any tier I/O so dirty vnodes are skipped cheaply.
    #[cfg(feature = "state-tier")]
    fn can_demote(&self, _vnode: u32, _vnode_count: u32) -> bool {
        false
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
        _deltas: &[(&[u8], &[u8])],
    ) -> Result<(), DbError> {
        Ok(())
    }

    /// Wire the cold-tier channel for vnode promotion. Only vnode-sharded
    /// aggregates use it; others ignore it.
    #[cfg(feature = "state-tier")]
    fn attach_state_tier(&mut self, _tier: crate::state_tier::TierTx) {}

    /// Vnodes this operator had demoted at the restored checkpoint; must be
    /// replayed from durable partials since the cold tier is wiped on restart.
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

// std HashMap: rkyv supports HashMap<K,V> natively but not FxHashMap; cold checkpoint path only.
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
            .apply("pre-filter", batches)
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
    max_state_bytes: Option<usize>,
    ctx: SessionContext,
    prom: Option<Arc<EngineMetrics>>,
    lookup_registry: Option<Arc<laminar_sql::datafusion::LookupTableRegistry>>,
    source_schemas: FxHashMap<String, SchemaRef>,
    temporal_configs: Vec<TemporalJoinTranslatorConfig>,
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
    // Plan-time errors from add_query (returns ()); surfaced by take_build_errors at start.
    build_errors: Vec<DbError>,
    #[cfg(feature = "cluster")]
    cluster_shuffle: Option<crate::operator::sql_query::ClusterShuffleConfig>,
    // `Some(chain_max)` enables incremental delta checkpoints on aggregate operators.
    #[cfg(feature = "cluster")]
    delta_chain_max: Option<u32>,
    // Set from the shuffle registry in cluster mode, or directly on a single-node tier path.
    #[cfg(feature = "cluster")]
    vnode_count: Option<u32>,
    // Staged per-vnode rehydration map; drained at the top of each cycle.
    #[cfg(feature = "cluster")]
    #[allow(clippy::disallowed_types)] // shares the DB's std-HashMap-typed handle
    rehydrated_vnode_state:
        Option<Arc<parking_lot::Mutex<std::collections::HashMap<u32, crate::db::RehydratedVnode>>>>,
    // Stored so DDL-added operators also receive the tier channel.
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
            max_state_bytes: None,
            #[cfg(feature = "cluster")]
            cluster_shuffle: None,
            #[cfg(feature = "cluster")]
            delta_chain_max: None,
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
            ports.iter().any(|port| !port.is_empty()) && !self.source_node_ids.contains(&id)
        })
    }

    /// Estimated state bytes per operator; cheap (operators maintain a counter).
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

    /// Install the cluster shuffle config for streaming aggregates.
    #[cfg(feature = "cluster")]
    pub fn set_cluster_shuffle(
        &mut self,
        config: crate::operator::sql_query::ClusterShuffleConfig,
    ) {
        self.vnode_count = Some(config.registry.vnode_count());
        self.cluster_shuffle = Some(config);
    }

    /// Enable incremental delta checkpoints on aggregate operators with `chain_max` as the bound.
    #[cfg(feature = "cluster")]
    pub fn set_delta_chain_max(&mut self, chain_max: u32) {
        self.delta_chain_max = Some(chain_max);
    }

    /// Set the vnode count for the single-node tier path (no shuffle config).
    /// Must stay stable across restarts; demoted partials are keyed by vnode.
    #[cfg(feature = "state-tier")]
    pub(crate) fn set_vnode_count(&mut self, vnode_count: u32) {
        self.vnode_count = Some(vnode_count);
    }

    /// Vnode count for per-vnode capture/demotion, if set.
    #[cfg(feature = "state-tier")]
    pub(crate) fn vnode_count(&self) -> Option<u32> {
        self.vnode_count
    }

    /// Wire the cold-tier channel to all current operators (and future DDL-added ones).
    #[cfg(feature = "state-tier")]
    pub(crate) fn set_state_tier(&mut self, tier: crate::state_tier::TierTx) {
        for node in &mut self.nodes {
            node.operator.attach_state_tier(tier.clone());
        }
        self.state_tier = Some(tier);
    }

    /// Cluster shuffle config, if installed; reused by the pipeline callback for subscriptions.
    #[cfg(feature = "cluster")]
    pub(crate) fn cluster_shuffle_config(
        &self,
    ) -> Option<&crate::operator::sql_query::ClusterShuffleConfig> {
        self.cluster_shuffle.as_ref()
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

        // Ownership may have changed again since staging; only drain what we own now.
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
            // Decode the recovery chain; an undecodable link is skipped (vnode resumes from current).
            let chain: Vec<crate::vnode_partial::VnodePartial> = rehydrated
                .chain
                .iter()
                .filter_map(|b| crate::vnode_partial::VnodePartial::decode(b).ok())
                .collect();
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
            let mut applied = 0usize;
            for op_name in &op_names {
                let Some((base, deltas)) =
                    crate::recovery_manager::resolve_op_chain(&chain, op_name)
                else {
                    continue; // no FULL base for this operator in the chain → start fresh
                };
                if let Some(node) = self
                    .nodes
                    .iter_mut()
                    .find(|n| !n.removed && &*n.name == op_name.as_str())
                {
                    if let Err(e) = node.operator.apply_vnode_chain(vnode, base, &deltas) {
                        tracing::warn!(
                            operator = %op_name, vnode, error = %e,
                            "failed to apply rehydrated vnode chain"
                        );
                    } else {
                        applied += 1;
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
                operators = applied,
                links = chain.len(),
                "applied rehydrated vnode chain"
            );
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
        self.input_sources.push(vec![usize::MAX]); // no upstream
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
            self.build_frame_operator_node(&name, &plan);
            return;
        }

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
                // Interval join first; falls back to processing-time equi-join.
                detect_stream_join_query(&sql).or_else(|| detect_processtime_join(&sql))
            } else {
                None
            };
        let stream_join_config = stream_join_detection.as_ref().map(|d| d.config.clone());
        let stream_join_projection_sql = stream_join_detection
            .as_ref()
            .map(|d| d.projection_sql.clone());

        // Lookup-enrich: only when no other specialized join matched.
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
        // Lookup-enrich reads its table from the registry, not as a graph input.
        if let Some(cfg) = &lookup_enrich_config {
            table_refs.remove(&cfg.table_name);
        }

        if let Some(ref tc) = temporal_config {
            self.temporal_configs.push(tc.clone());
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
            idle_ttl_ms,
        );

        let input_port_count = if asof_config.is_some()
            || stream_join_config.is_some()
            || temporal_probe_config.is_some()
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

    // Replace a SourcePassthrough placeholder in place (preserving its id and outbound edges),
    // or append a fresh node. Callers must ensure source nodes before and wire edges after.
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
            self.source_node_ids.remove(&id);
            // Downstream nodes already wired to the placeholder now depend on this query.
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
        let node_id = self.place_operator_node(name, operator, 1);
        let depends = self.wire_query_edges(node_id, None, None, None, None, None, &table_refs);
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

        // Falls through to the DataFusion lookup path if the registry/handle is absent.
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
                // Key-shard the probe side for cache affinity.
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
            // No time columns → per-cycle batch join; with time columns → interval join.
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
            // Delta checkpoints are a cluster (per-vnode) capability — only wire when sharded.
            if let Some(chain_max) = self.delta_chain_max {
                op.enable_delta_checkpoints(chain_max);
            }
        }
        #[cfg(feature = "state-tier")]
        if let Some(tier) = self.state_tier.clone() {
            op.attach_state_tier(tier);
            // Single-node path has no shuffle config to read the count from.
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

        for node in &mut self.nodes {
            node.output_routes
                .retain(|&(t, _)| !ids_to_remove.contains(&t));
        }

        self.output_map.remove(name);
        self.topo_dirty = true;
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

        self.source_list.clear();
        self.source_list
            .extend(self.source_map.iter().map(|(k, v)| (Arc::clone(k), *v)));

        self.output_node_ids.clear();
        self.output_node_ids
            .extend(self.output_map.values().copied());

        self.topo_dirty = false;
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

        self.propagate_operator_watermark(node_id, &watermarks, current_watermark);

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
                }
                b
            }
            Err(e) => {
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
                for v in &mut inputs {
                    v.clear();
                }
                input_bytes.fill(0);
                self.input_bufs[node_id] = inputs;
                self.input_buf_bytes[node_id] = input_bytes;
                return Err(e);
            }
        };

        self.enforce_state_limit(node_id)?;

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

    fn enforce_state_limit(&self, node_id: usize) -> Result<(), DbError> {
        let Some(limit) = self.max_state_bytes else {
            return Ok(());
        };
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
        Ok(())
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
        #[cfg(feature = "cluster")]
        self.apply_rehydrated_vnodes();

        if self.topo_dirty {
            self.compute_topo_order();
        }

        self.register_source_tables(source_batches);
        self.prime_sources(source_batches, current_watermark, source_watermarks);

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

            if i > 0 {
                #[allow(clippy::cast_possible_truncation)]
                let elapsed_ns = cycle_start.elapsed().as_nanos() as u64;
                if elapsed_ns > self.query_budget_ns {
                    tracing::debug!(
                        skipped = topo_len - i,
                        elapsed_ms = elapsed_ns / 1_000_000,
                        "per-query budget exceeded — deferring remaining operators"
                    );

                    if let Err(e) = self
                        .run_one_deferred_operator(i, topo_len, current_watermark, &mut results)
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
                self.finish_cycle();
                return Err(e);
            }
        }

        self.finish_cycle();

        #[cfg(debug_assertions)]
        self.debug_assert_byte_sums();

        self.sample_buffer_stats();

        Ok(results)
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
    async fn run_one_deferred_operator(
        &mut self,
        i: usize,
        topo_len: usize,
        current_watermark: i64,
        results: &mut FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<(), DbError> {
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
                    return Err(DbError::BackpressureFail(format!(
                        "input buffer at capacity downstream of '{}'",
                        self.nodes[deferred_id].name
                    )));
                }
                GateDecision::Run => {}
            }
            self.execute_single_operator(deferred_id, current_watermark, results)
                .await?;
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

    // Unknown stage (no live operator for it) is silently dropped.
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

    /// Chandy–Lamport shuffle alignment: fan out a barrier to peers, drain rows,
    /// and wait until every peer's barrier is observed before snapshotting.
    ///
    /// Requires full-membership commit — no peer resumes the next epoch until all
    /// have aligned, so no next-epoch frame can arrive mid-alignment.
    ///
    /// # Errors
    /// Fails the checkpoint on timeout or closed receiver so the caller can retry.
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
        use laminar_core::shuffle::ShuffleMessage;
        use rustc_hash::FxHashSet;

        // Safety cap only — the membership self-heal below normally finishes alignment
        // well before this by dropping any peer that leaves membership mid-align.
        const ALIGN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);
        const RECHECK: std::time::Duration = std::time::Duration::from_millis(500);

        let Some(cfg) = self.cluster_shuffle.clone() else {
            return Ok(());
        };
        // Fan-out and wait set both come from the same `live` membership so they stay
        // mutually consistent across nodes (a vnode-ownership fan-out could target a
        // stale/dead owner a live peer is still waiting on). Barriers are cheap.
        let peers: Vec<u64> = live
            .iter()
            .copied()
            .filter(|&id| id != cfg.self_id.0)
            .collect();
        if peers.is_empty() {
            return Ok(());
        }

        // Pre-staged rows arrived before the barrier; fold them in first.
        for (stage, batch) in cfg.receiver.drain_all_staged() {
            self.ingest_to_stage(&stage, batch, watermark).await?;
        }

        let barrier = CheckpointBarrier::new(checkpoint_id, 0);
        cfg.sender
            .fan_out_barrier(&peers, barrier)
            .await
            .map_err(|e| DbError::Pipeline(format!("shuffle barrier fan-out: {e}")))?;

        // Self-healing wait set: done when every peer's barrier is seen OR the peer has
        // left membership (re-checked each tick) — a peer that dies can't wedge the epoch.
        let mut remaining: FxHashSet<u64> = peers.iter().copied().collect();
        tracing::debug!(checkpoint_id, self_id = cfg.self_id.0, peers = ?peers, "shuffle align: start");

        // Barriers stashed before we began aligning. A later-checkpoint barrier (a faster
        // peer that moved on) is re-stashed, not dropped, so we still see it at that epoch.
        for (from, b) in cfg.receiver.drain_staged_barriers() {
            if b.checkpoint_id == checkpoint_id {
                remaining.remove(&from);
            } else if b.checkpoint_id > checkpoint_id {
                cfg.receiver.stash_barrier(from, b);
            }
        }
        if remaining.is_empty() {
            return Ok(());
        }

        let alignment_timeout = tokio::time::sleep(ALIGN_TIMEOUT);
        tokio::pin!(alignment_timeout);
        let mut check_interval = tokio::time::interval(RECHECK);
        loop {
            tokio::select! {
                res = cfg.receiver.recv() => {
                    match res {
                        None => {
                            return Err(DbError::Pipeline(
                                "shuffle receiver closed during barrier alignment".into(),
                            ));
                        }
                        Some((from, ShuffleMessage::Barrier(b))) => {
                            if b.checkpoint_id == checkpoint_id {
                                if remaining.remove(&from) && remaining.is_empty() {
                                    break;
                                }
                            } else if b.checkpoint_id > checkpoint_id {
                                cfg.receiver.stash_barrier(from, b); // for a later epoch; keep it
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
                        // Stop waiting on peers that left membership (a dead peer never
                        // sends its barrier); Active-but-slow peers stay in the wait set.
                        let live_now: FxHashSet<u64> =
                            ctrl.live_instances().iter().map(|n| n.0).collect();
                        remaining.retain(|p| live_now.contains(p));
                        if remaining.is_empty() {
                            break;
                        }
                        if let Ok(Some(ann)) = ctrl.observe_barrier().await {
                            if ann.checkpoint_id == checkpoint_id && ann.phase == Phase::Abort {
                                return Err(DbError::Pipeline(format!(
                                    "checkpoint {checkpoint_id} was aborted by leader"
                                )));
                            }
                            // A newer announcement means the leader moved on; our barriers will
                            // never arrive, and without this check a rejoining node livelocks.
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
                        "shuffle barrier alignment timed out for checkpoint {checkpoint_id} \
                         (waiting on {remaining:?})"
                    )));
                }
            }
        }
        tracing::debug!(checkpoint_id, "shuffle align: complete");
        Ok(())
    }

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

    /// Drop one vnode's state after its cold-tier write is confirmed.
    /// Returns `false` if the operator refuses (vnode modified since last capture).
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
        if demoted {
            if let Some(ref prom) = self.prom {
                prom.state_tier_demote_total.inc();
            }
        }
        demoted
    }

    /// Whether the named operator could demote `vnode` right now (no tier I/O performed).
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

    /// Vnodes each operator had demoted at the last checkpoint; absent from the manifest
    /// and must be replayed from durable partials. Returns `(operator_name, vnodes)`.
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

    /// Replay one operator's recovery chain (FULL base + ordered deltas) for a vnode (cold-vnode
    /// rehydration on restart). Targets a single operator to avoid double-applying manifest slices.
    #[cfg(feature = "state-tier")]
    pub(crate) fn apply_vnode_chain(
        &mut self,
        operator: &str,
        vnode: u32,
        base: &[u8],
        deltas: &[(&[u8], &[u8])],
    ) -> Result<(), DbError> {
        match self
            .nodes
            .iter_mut()
            .find(|n| !n.removed && &*n.name == operator)
        {
            Some(node) => node.operator.apply_vnode_chain(vnode, base, deltas),
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
