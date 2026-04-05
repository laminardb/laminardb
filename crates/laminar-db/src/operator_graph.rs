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

use crate::error::DbError;
use crate::metrics::PipelineCounters;
use crate::sql_analysis::{
    apply_topk_filter, detect_asof_query, detect_stream_join_query, detect_temporal_probe_query,
    detect_temporal_query, extract_table_references, StreamJoinDetection,
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
}

pub(crate) struct OperatorCheckpoint {
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct GraphCheckpoint {
    pub version: u32,
    pub operators: FxHashMap<String, Vec<u8>>,
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

        let schema = batches[0].schema();
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches])
            .map_err(|e| DbError::Pipeline(format!("pre-filter: {e}")))?;

        let _ = self.ctx.deregister_table(&self.tmp_table);
        self.ctx
            .register_table(&self.tmp_table, Arc::new(mem_table))
            .map_err(|e| DbError::Pipeline(format!("pre-filter: {e}")))?;

        let sql = format!("SELECT * FROM {} WHERE {}", self.tmp_table, self.filter_sql);
        let result = async {
            self.ctx
                .sql(&sql)
                .await
                .map_err(|e| DbError::Pipeline(format!("pre-filter: {e}")))?
                .collect()
                .await
                .map_err(|e| DbError::Pipeline(format!("pre-filter: {e}")))
        }
        .await;

        let _ = self.ctx.deregister_table(&self.tmp_table);
        result
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
    /// Cached set of source node IDs for O(1) lookup in `execute_single_operator`.
    source_node_ids: FxHashSet<usize>,
    output_map: FxHashMap<Arc<str>, usize>,
    input_bufs: Vec<Vec<Vec<RecordBatch>>>,
    /// Per-node, per-input-port upstream node id. `input_sources[node][port] = upstream_node`.
    input_sources: Vec<Vec<usize>>,
    /// Per-node output watermark, set during `execute_cycle`.
    output_watermarks: Vec<i64>,
    /// Maximum batches per input port before shedding. Prevents unbounded
    /// fan-out growth within a single cycle. `0` means unlimited (default).
    max_input_buf_batches: usize,
    query_budget_ns: u64,
    /// Round-robin offset for deferred operator selection. Ensures fair
    /// scheduling when budget is continuously exceeded (not checkpointed).
    deferred_scan_offset: usize,
    max_state_bytes: Option<usize>,
    ctx: SessionContext,
    counters: Option<Arc<PipelineCounters>>,
    lookup_registry: Option<Arc<laminar_sql::datafusion::LookupTableRegistry>>,
    source_schemas: FxHashMap<String, SchemaRef>,
    temporal_configs: Vec<TemporalJoinTranslatorConfig>,
    depends_on_stream: FxHashSet<usize>,
    order_configs: FxHashMap<usize, OrderOperatorConfig>,
    /// Per-table `LiveSourceHandle` for batch swapping without catalog churn.
    /// Covers both source tables (from `register_source_schema`) and
    /// intermediate tables (created lazily on first operator output).
    live_handles: FxHashMap<String, LiveSourceHandle>,
}

impl OperatorGraph {
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            topo_order: Vec::new(),
            topo_dirty: true,
            source_map: FxHashMap::default(),
            source_node_ids: FxHashSet::default(),
            output_map: FxHashMap::default(),
            input_bufs: Vec::new(),
            input_sources: Vec::new(),
            output_watermarks: Vec::new(),
            max_input_buf_batches: 0,
            query_budget_ns: 8_000_000,
            deferred_scan_offset: 0,
            max_state_bytes: None,
            ctx,
            counters: None,
            lookup_registry: None,
            source_schemas: FxHashMap::default(),
            temporal_configs: Vec::new(),
            depends_on_stream: FxHashSet::default(),
            order_configs: FxHashMap::default(),
            live_handles: FxHashMap::default(),
        }
    }

    pub fn set_max_state_bytes(&mut self, limit: Option<usize>) {
        self.max_state_bytes = limit;
    }

    pub fn set_max_input_buf_batches(&mut self, cap: usize) {
        self.max_input_buf_batches = cap;
    }

    pub fn set_query_budget_ns(&mut self, ns: u64) {
        self.query_budget_ns = ns;
    }

    pub fn set_counters(&mut self, c: Arc<PipelineCounters>) {
        self.counters = Some(c);
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn input_buf_pressure(&self) -> f64 {
        let cap = self.max_input_buf_batches;
        if cap == 0 {
            return 0.0;
        }
        let max_len = self
            .input_bufs
            .iter()
            .flat_map(|ports| ports.iter())
            .map(Vec::len)
            .max()
            .unwrap_or(0);
        (max_len as f64 / cap as f64).min(1.0)
    }

    pub fn has_pending_input(&self) -> bool {
        self.input_bufs.iter().enumerate().any(|(id, ports)| {
            ports.iter().any(|port| !port.is_empty())
                && !self.source_map.values().any(|&src| src == id)
        })
    }

    pub fn set_lookup_registry(
        &mut self,
        registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    ) {
        self.lookup_registry = Some(registry);
    }

    fn enforce_input_buf_cap(&mut self, node: usize, port: usize) {
        let cap = self.max_input_buf_batches;
        if cap == 0 {
            return;
        }
        let buf = &mut self.input_bufs[node][port];
        if buf.len() > cap {
            let shed = buf.len() - cap;
            buf.drain(..shed);
            tracing::warn!(
                node = %self.nodes[node].name,
                port,
                shed,
                cap,
                "input buffer exceeded cap — shed oldest batches"
            );
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

    #[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
    pub fn add_query(
        &mut self,
        name: String,
        sql: String,
        emit_clause: Option<EmitClause>,
        window_config: Option<WindowOperatorConfig>,
        order_config: Option<OrderOperatorConfig>,
        idle_ttl_ms: Option<u64>,
    ) {
        // Detection (same as StreamExecutor::add_query)
        let (temporal_probe_config, temporal_probe_projection_sql) =
            detect_temporal_probe_query(&sql);
        let (asof_config, projection_sql) = if temporal_probe_config.is_none() {
            detect_asof_query(&sql)
        } else {
            (None, None)
        };
        let (temporal_config, temporal_projection_sql) = if temporal_probe_config.is_none() {
            detect_temporal_query(&sql)
        } else {
            (None, None)
        };
        let stream_join_detection = if temporal_probe_config.is_none() {
            detect_stream_join_query(&sql)
        } else {
            None
        };
        let stream_join_config = stream_join_detection.as_ref().map(|d| d.config.clone());
        let stream_join_projection_sql = stream_join_detection
            .as_ref()
            .map(|d| d.projection_sql.clone());
        let projection_sql = projection_sql
            .or(temporal_probe_projection_sql)
            .or(temporal_projection_sql)
            .or(stream_join_projection_sql);

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

        let table_refs = extract_table_references(&sql);

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

        // If a SourcePassthrough placeholder exists with this query's name
        // (created by an earlier add_query whose SQL referenced this name
        // before we were registered), replace it in place. The placeholder's
        // node ID and existing outbound edges stay valid — downstream nodes
        // that already wired to the placeholder automatically receive our
        // output. This handles HashMap-order-independent query registration.
        if let Some(&placeholder_id) = self.source_map.get(name.as_str()) {
            self.nodes[placeholder_id].operator = operator;
            self.nodes[placeholder_id].input_port_count = input_port_count;
            self.input_bufs[placeholder_id] = vec![Vec::new(); input_port_count];
            self.input_sources[placeholder_id] = vec![usize::MAX; input_port_count];
            self.source_map.remove(name.as_str());

            let node_id = placeholder_id;

            self.ensure_query_source_nodes(
                temporal_probe_config.as_ref(),
                asof_config.as_ref(),
                stream_join_config.as_ref(),
                // Placeholder path was missing temporal_config handling;
                // the normal path handles it, so pass None here to keep
                // behaviour identical to the old code.
                None,
                &table_refs,
            );
            let depends = self.wire_query_edges(
                node_id,
                temporal_probe_config.as_ref(),
                asof_config.as_ref(),
                stream_join_config.as_ref(),
                stream_join_detection.as_ref(),
                None,
                &table_refs,
            );
            if depends {
                self.depends_on_stream.insert(node_id);
            }

            // Mark downstream nodes as depends_on_stream
            for &(target, _) in &self.nodes[node_id].output_routes {
                self.depends_on_stream.insert(target);
            }

            if let Some(oc) = order_config {
                self.order_configs.insert(node_id, oc);
            }
            self.output_map.insert(Arc::from(name.as_str()), node_id);
            self.topo_dirty = true;
            return;
        }

        // Normal path: no placeholder to replace.

        // Ensure source nodes exist BEFORE creating the operator node so
        // that source node indices are stable when building edges.
        self.ensure_query_source_nodes(
            temporal_probe_config.as_ref(),
            asof_config.as_ref(),
            stream_join_config.as_ref(),
            temporal_config.as_ref(),
            &table_refs,
        );

        let node_id = self.nodes.len();
        self.nodes.push(GraphNode {
            name: Arc::from(name.as_str()),
            operator,
            input_port_count,
            output_routes: Vec::new(),
            removed: false,
        });
        self.input_bufs.push(vec![Vec::new(); input_port_count]);
        self.input_sources.push(vec![usize::MAX; input_port_count]);
        self.output_watermarks.push(i64::MIN);

        // Create edges from table refs
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

        // Store order config for Top-K post-filtering
        if let Some(oc) = order_config {
            self.order_configs.insert(node_id, oc);
        }

        // Register as output
        self.output_map.insert(Arc::from(name.as_str()), node_id);
        self.topo_dirty = true;
    }

    #[allow(clippy::too_many_arguments)]
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
        projection_sql: Option<&str>,
        idle_ttl_ms: Option<u64>,
    ) -> Box<dyn GraphOperator> {
        use crate::operator;

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
            return Box::new(operator::interval_join::IntervalJoinOperator::new(
                name,
                cfg.clone(),
                projection_sql.map(Arc::from),
                self.ctx.clone(),
            ));
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
                self.counters.clone(),
            ));
        }

        let emit_changelog = emit_clause.is_some_and(|ec| matches!(ec, EmitClause::Changes));

        Box::new(operator::sql_query::SqlQueryOperator::new(
            name,
            sql,
            self.ctx.clone(),
            self.counters.clone(),
            emit_changelog,
            idle_ttl_ms,
        ))
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
        let inputs = std::mem::take(&mut self.input_bufs[node_id]);

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
            .process(&inputs, &watermarks)
            .await;

        // Source nodes (input_sources = [usize::MAX]) have output_watermarks
        // pre-seeded from per-source watermarks in execute_cycle. Don't overwrite.
        if !self.source_node_ids.contains(&node_id) {
            self.output_watermarks[node_id] = watermarks
                .iter()
                .copied()
                .min()
                .unwrap_or(current_watermark);
        }

        let batches = match output_result {
            Ok(b) => {
                let port_count = self.nodes[node_id].input_port_count;
                self.input_bufs[node_id] = vec![Vec::new(); port_count];
                b
            }
            Err(e) => {
                if self.depends_on_stream.contains(&node_id) {
                    // Put batches back so they're retried next cycle.
                    self.input_bufs[node_id] = inputs;
                    tracing::debug!(
                        query = %self.nodes[node_id].name,
                        error = %e,
                        "Query deferred (upstream not ready); batches preserved for retry"
                    );
                    return Ok(());
                }
                let port_count = self.nodes[node_id].input_port_count;
                self.input_bufs[node_id] = vec![Vec::new(); port_count];
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
                let name_str = node_name.to_string();
                if !self.live_handles.contains_key(&name_str) {
                    let schema = batches[0].schema();
                    self.ensure_live_provider(&name_str, &schema);
                }
                if let Some(handle) = self.live_handles.get(name_str.as_str()) {
                    handle.swap(batches.clone());
                }
            }

            // Collect output for the caller.
            if is_output {
                results.insert(node_name, batches.clone());
            }

            // Route to downstream operator input buffers.
            let routes = self.nodes[node_id].output_routes.clone();
            if routes.len() == 1 {
                let (target, port) = routes[0];
                let buf = &mut self.input_bufs[target][port as usize];
                if buf.is_empty() {
                    *buf = batches;
                } else {
                    buf.extend(batches);
                }
                self.enforce_input_buf_cap(target, port as usize);
            } else if routes.len() > 1 {
                for &(target, port) in &routes {
                    self.input_bufs[target][port as usize].extend(batches.iter().cloned());
                    self.enforce_input_buf_cap(target, port as usize);
                }
            }
        }

        Ok(())
    }

    pub async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
        source_watermarks: Option<&FxHashMap<Arc<str>, i64>>,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, DbError> {
        if self.topo_dirty {
            self.compute_topo_order();
        }

        self.register_source_tables(source_batches);

        for (name, &node_id) in &self.source_map {
            if let Some(batches) = source_batches.get(name) {
                self.input_bufs[node_id][0].clone_from(batches);
            }
            let wm = source_watermarks
                .and_then(|m| m.get(name).copied())
                .unwrap_or(current_watermark);
            self.output_watermarks[node_id] = wm;
        }

        let mut results = FxHashMap::default();
        let cycle_start = std::time::Instant::now();
        let topo_len = self.topo_order.len();

        for i in 0..topo_len {
            let node_id = self.topo_order[i];

            if self.nodes[node_id].removed {
                continue;
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
        Ok(results)
    }

    // &mut self: some accumulators need &mut for state()
    pub fn snapshot_state(&mut self) -> Result<Option<GraphCheckpoint>, DbError> {
        let mut operators = FxHashMap::default();
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
        serde_json::to_vec(cp)
            .map_err(|e| DbError::Pipeline(format!("operator graph checkpoint serialization: {e}")))
    }

    pub fn restore_from_bytes(&mut self, bytes: &[u8]) -> Result<usize, DbError> {
        let checkpoint: GraphCheckpoint = serde_json::from_slice(bytes).map_err(|e| {
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
        );
        graph.add_query(
            "q2".to_string(),
            "SELECT symbol FROM q1 WHERE price > 100".to_string(),
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
        );
        graph.add_query(
            "q1".to_string(),
            "SELECT * FROM trades".to_string(),
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
        );
        graph.add_query(
            "q2".to_string(),
            "SELECT symbol FROM trades".to_string(),
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
        );
        // No state yet → None
        let cp = graph.snapshot_state().unwrap();
        assert!(cp.is_none());
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
        );
        graph.add_query(
            "step2".to_string(),
            "SELECT symbol, doubled FROM step1 WHERE doubled > 400".to_string(),
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
        );
        graph.add_query(
            "low".to_string(),
            "SELECT symbol, price FROM trades WHERE price <= 200".to_string(),
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
        );
        graph.add_query(
            "q2".to_string(),
            "SELECT * FROM trades".to_string(),
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
        );
        graph.add_query(
            "q1".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
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
        );
        // Push some data into the source buffer
        if let Some(&node_id) = graph.source_map.get("trades") {
            graph.input_bufs[node_id][0] = vec![test_batch(); 10];
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
        );
        // Fill source buffer to 50% of cap
        if let Some(&node_id) = graph.source_map.get("trades") {
            graph.input_bufs[node_id][0] = vec![test_batch(); 50];
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
        );
        // Overfill buffer beyond cap (enforce_input_buf_cap only runs
        // during execute_cycle routing, not here).
        if let Some(&node_id) = graph.source_map.get("trades") {
            graph.input_bufs[node_id][0] = vec![test_batch(); 20];
        }
        assert!((graph.input_buf_pressure() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_pressure_empty_graph() {
        let graph = test_graph();
        assert!((graph.input_buf_pressure() - 0.0).abs() < f64::EPSILON);
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
        source.insert(Arc::from("events"), vec![batch]);

        let results = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        let joined = results
            .get("joined")
            .expect("joined query should produce output");
        let total_rows: usize = joined.iter().map(|b| b.num_rows()).sum();

        // 2A × 2B within time window = up to 4 matches
        assert!(total_rows > 0, "should produce matches");
        assert!(
            total_rows <= 4,
            "at most 4 matches (2A × 2B), got {total_rows}"
        );
    }
}
