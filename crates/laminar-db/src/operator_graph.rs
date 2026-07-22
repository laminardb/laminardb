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
use crate::operator::capability::{OperatorCapability, OperatorImplementation};
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
    /// Admission-neutral inventory of this implementation's current cluster state shape.
    ///
    /// This method is intentionally mandatory: a new physical operator must be classified before
    /// it compiles. Cluster DDL admission does not consume this descriptor yet.
    fn cluster_capability(&self) -> OperatorCapability;

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

impl GraphNode {
    fn new(name: Arc<str>, operator: Box<dyn GraphOperator>, input_port_count: usize) -> Self {
        let capability = operator.cluster_capability();
        tracing::debug!(
            operator = %name,
            implementation = ?capability.implementation,
            state_class = ?capability.state_class,
            cluster_status = ?capability.cluster_status,
            "registered physical operator capability inventory"
        );
        Self {
            name,
            operator,
            input_port_count,
            output_routes: Vec::new(),
            removed: false,
        }
    }

    fn replace_operator(&mut self, operator: Box<dyn GraphOperator>) {
        let capability = operator.cluster_capability();
        tracing::debug!(
            operator = %self.name,
            implementation = ?capability.implementation,
            state_class = ?capability.state_class,
            cluster_status = ?capability.cluster_status,
            "replaced physical operator capability inventory"
        );
        self.operator = operator;
    }
}

struct GraphEdge {
    source: usize,
    target: usize,
}

struct SourcePassthrough;

#[async_trait]
impl GraphOperator for SourcePassthrough {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::fixed(OperatorImplementation::SourcePassthrough)
    }

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
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::fixed(OperatorImplementation::Tombstoned)
    }

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
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::fixed(OperatorImplementation::SqlFilter)
    }

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
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::fixed(OperatorImplementation::ChangelogEnrich)
    }

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
        let node_id = self.allocate_node(GraphNode::new(
            Arc::clone(&name),
            Box::new(SourcePassthrough),
            1,
        ));
        self.source_map.insert(name, node_id);
        self.source_node_ids.insert(node_id);
        node_id
    }

    fn insert_filter_node(&mut self, name: &str, filter_sql: String, source_id: usize) -> usize {
        let node_id = self.allocate_node(GraphNode::new(
            Arc::from(name),
            Box::new(SqlFilterOperator::new(filter_sql, self.ctx.clone(), name)),
            1,
        ));
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
            self.build_frame_operator_node(&name, &plan);
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
            self.nodes[id].replace_operator(operator);
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
            self.allocate_node(GraphNode::new(Arc::from(name), operator, input_port_count))
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
        let node_id = self.place_prepared_operator_node(name, operator, 1);
        let depends =
            self.wire_query_edges(node_id, None, None, None, None, None, None, &table_refs);
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

        let op = operator::sql_query::SqlQueryOperator::new(
            name,
            sql,
            self.ctx.clone(),
            self.prom.clone(),
            emit_changelog,
        );
        #[cfg(feature = "cluster")]
        let mut op = op;
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
            self.nodes[id].replace_operator(Box::new(TombstonedOperator));
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

    #[cfg(feature = "cluster")]
    fn ensure_shuffle_delivery_intact(
        cfg: &crate::operator::sql_query::ClusterShuffleConfig,
    ) -> Result<(), DbError> {
        if cfg.receiver.has_unrecovered_delivery_loss() {
            Err(DbError::Pipeline(
                "shuffle delivery-domain or transit loss requires recovery".into(),
            ))
        } else {
            Ok(())
        }
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_remaining_shuffle_barriers(
        &mut self,
        cfg: &crate::operator::sql_query::ClusterShuffleConfig,
        attempt: laminar_core::state::CheckpointAttempt,
        watermark: i64,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        deadline: tokio::time::Instant,
        controller: Option<&laminar_core::cluster::control::ClusterController>,
        recovery_gen: u64,
        mut remaining: rustc_hash::FxHashSet<u64>,
        mut barrier_cuts: rustc_hash::FxHashMap<u64, u64>,
    ) -> Result<ShuffleAlignmentOutcome, DbError> {
        use laminar_core::shuffle::ShuffleMessage;

        const RECHECK: std::time::Duration = std::time::Duration::from_millis(500);
        let mut check_interval =
            tokio::time::interval_at(tokio::time::Instant::now() + RECHECK, RECHECK);
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
                        cfg,
                        assignment_fence,
                        recovery_gen,
                        controller,
                    )?;
                    Self::ensure_shuffle_delivery_intact(cfg)?;
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
                        cfg,
                        assignment_fence,
                        recovery_gen,
                        controller,
                    )?;
                    Self::ensure_shuffle_delivery_intact(cfg)?;
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
        Self::validate_shuffle_attempt_scope(cfg, assignment_fence, recovery_gen, controller)?;
        Self::ensure_shuffle_delivery_intact(cfg)?;
        if let Some(outcome) =
            Self::audit_shuffle_alignment_settlement(controller, attempt, assignment_fence).await?
        {
            return Ok(outcome);
        }
        tracing::debug!(
            checkpoint_id = attempt.checkpoint_id,
            epoch = attempt.epoch,
            "shuffle align: complete"
        );
        Ok(ShuffleAlignmentOutcome::Aligned)
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
            Self::ensure_shuffle_delivery_intact(&cfg)?;
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
            Self::ensure_shuffle_delivery_intact(&cfg)?;

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
            Self::ensure_shuffle_delivery_intact(&cfg)?;
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
                Self::ensure_shuffle_delivery_intact(&cfg)?;
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

            return self
                .wait_for_remaining_shuffle_barriers(
                    &cfg,
                    attempt,
                    watermark,
                    assignment_fence,
                    deadline,
                    controller,
                    recovery_gen,
                    remaining,
                    barrier_cuts,
                )
                .await;
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
        self.allocate_node(GraphNode::new(Arc::from(name), operator, 1));
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
mod tests;
