//! DAG topology data structures.
//!
//! Defines `DagNode`, `DagEdge`, and `StreamingDag` with topological ordering,
//! cycle detection, and automatic channel type derivation.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use super::error::DagError;

/// Maximum fan-out targets per node output port.
///
/// Matches the routing table entry size.
pub const MAX_FAN_OUT: usize = 8;

/// Unique identifier for a node in the DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u32);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}

/// Unique identifier for an edge in the DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeId(pub u32);

impl fmt::Display for EdgeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EdgeId({})", self.0)
    }
}

/// Unique identifier for a state partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatePartitionId(pub u32);

/// Classification of a DAG node for ring assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DagNodeType {
    /// External data source (Connector SDK source, Ring 1).
    Source,
    /// Stateless operator (map, filter, project -- Ring 0).
    StatelessOperator,
    /// Stateful operator (window, join, aggregate -- Ring 0).
    StatefulOperator,
    /// Materialized view (wraps `MvPipelineExecutor` node -- Ring 0).
    MaterializedView,
    /// External data sink (Connector SDK sink, Ring 1).
    Sink,
}

/// Channel type derived from DAG topology analysis.
///
/// Extends the existing `ChannelMode` from `streaming::channel`
/// (which supports SPSC and MPSC) with SPMC for fan-out.
/// Channel types are NEVER user-specified; they are computed from
/// the fan-in and fan-out of connected nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DagChannelType {
    /// Single producer, single consumer (optimal for linear segments).
    Spsc,
    /// Single producer, multiple consumers (fan-out / shared stage).
    Spmc,
    /// Multiple producers, single consumer (fan-in).
    Mpsc,
}

/// Custom partitioning function type.
pub type PartitionFn = Arc<dyn Fn(&[u8]) -> usize + Send + Sync>;

/// Partitioning strategy for parallel operator instances.
#[derive(Clone, Default)]
pub enum PartitioningStrategy {
    /// All events go to a single partition (no parallelism).
    #[default]
    Single,
    /// Round-robin distribution across partitions.
    RoundRobin,
    /// Hash-based partitioning by key expression.
    HashBy(String),
    /// Custom partitioning function.
    Custom(PartitionFn),
}

impl fmt::Debug for PartitioningStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Single => write!(f, "Single"),
            Self::RoundRobin => write!(f, "RoundRobin"),
            Self::HashBy(key) => write!(f, "HashBy({key})"),
            Self::Custom(_) => write!(f, "Custom(...)"),
        }
    }
}

/// A node in the DAG represents an operator or stage.
///
/// Nodes are created during topology construction in Ring 2
/// and are immutable once the DAG is finalized.
pub struct DagNode {
    /// Unique node identifier.
    pub id: NodeId,
    /// Human-readable name (e.g., "normalize", "vwap").
    pub name: String,
    /// Upstream connections (fan-in). `SmallVec` avoids heap alloc for <= 4 inputs.
    pub inputs: SmallVec<[EdgeId; 4]>,
    /// Downstream connections (fan-out). `SmallVec` avoids heap alloc for <= 4 outputs.
    pub outputs: SmallVec<[EdgeId; 4]>,
    /// Output schema for downstream type checking.
    pub output_schema: SchemaRef,
    /// State partition assignment (for thread-per-core routing).
    pub state_partition: StatePartitionId,
    /// Node classification for ring assignment.
    pub node_type: DagNodeType,
}

impl fmt::Debug for DagNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DagNode")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("inputs", &self.inputs)
            .field("outputs", &self.outputs)
            .field("node_type", &self.node_type)
            .field("state_partition", &self.state_partition)
            .finish_non_exhaustive()
    }
}

/// An edge represents a data flow connection between two nodes.
///
/// The channel type is automatically derived from the topology,
/// consistent with the auto-upgrade pattern in `streaming::channel`.
#[derive(Debug)]
pub struct DagEdge {
    /// Unique edge identifier.
    pub id: EdgeId,
    /// Source node.
    pub source: NodeId,
    /// Target node.
    pub target: NodeId,
    /// Channel type (derived from topology analysis, never user-specified).
    pub channel_type: DagChannelType,
    /// Partitioning strategy for parallel execution.
    pub partitioning: PartitioningStrategy,
    /// Output port on the source node (for multi-output operators).
    pub source_port: u8,
    /// Input port on the target node (for multi-input operators).
    pub target_port: u8,
}

/// Metadata for a shared intermediate stage.
///
/// Created during topology finalization for any node with fan-out > 1.
#[derive(Debug)]
pub struct SharedStageMetadata {
    /// Node that produces the shared output.
    pub producer_node: NodeId,
    /// Number of downstream consumers.
    pub consumer_count: usize,
    /// Consumer node IDs.
    pub consumer_nodes: Vec<NodeId>,
}

/// The complete DAG topology.
///
/// Constructed in Ring 2 via `DagBuilder` or from SQL `CREATE MATERIALIZED VIEW`
/// chains. Once built, the topology is immutable and can be executed in Ring 0.
pub struct StreamingDag {
    /// All nodes in the DAG, keyed by `NodeId`.
    nodes: FxHashMap<NodeId, DagNode>,
    /// All edges in the DAG, keyed by `EdgeId`.
    edges: FxHashMap<EdgeId, DagEdge>,
    /// Topologically sorted execution order (dependencies first).
    /// Computed via Kahn's algorithm.
    execution_order: Vec<NodeId>,
    /// Shared intermediate stage metadata (nodes with fan-out > 1).
    shared_stages: FxHashMap<NodeId, SharedStageMetadata>,
    /// Source nodes (entry points, no inputs).
    source_nodes: Vec<NodeId>,
    /// Sink nodes (exit points, no outputs).
    sink_nodes: Vec<NodeId>,
    /// Name -> `NodeId` index for lookups.
    name_index: FxHashMap<String, NodeId>,
    /// Next node ID counter.
    next_node_id: u32,
    /// Next edge ID counter.
    next_edge_id: u32,
    /// Whether the DAG has been finalized.
    finalized: bool,
}

impl fmt::Debug for StreamingDag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingDag")
            .field("node_count", &self.nodes.len())
            .field("edge_count", &self.edges.len())
            .field("source_nodes", &self.source_nodes)
            .field("sink_nodes", &self.sink_nodes)
            .field("execution_order", &self.execution_order)
            .field("finalized", &self.finalized)
            .finish_non_exhaustive()
    }
}

impl StreamingDag {
    /// Creates a new empty DAG.
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: FxHashMap::default(),
            edges: FxHashMap::default(),
            execution_order: Vec::new(),
            shared_stages: FxHashMap::default(),
            source_nodes: Vec::new(),
            sink_nodes: Vec::new(),
            name_index: FxHashMap::default(),
            next_node_id: 0,
            next_edge_id: 0,
            finalized: false,
        }
    }

    /// Adds a node to the DAG.
    ///
    /// # Errors
    ///
    /// Returns `DagError::DuplicateNode` if a node with the same name exists.
    pub fn add_node(
        &mut self,
        name: impl Into<String>,
        node_type: DagNodeType,
        output_schema: SchemaRef,
    ) -> Result<NodeId, DagError> {
        let name = name.into();
        if self.name_index.contains_key(&name) {
            return Err(DagError::DuplicateNode(name));
        }

        let id = NodeId(self.next_node_id);
        self.next_node_id += 1;

        let node = DagNode {
            id,
            name: name.clone(),
            inputs: SmallVec::new(),
            outputs: SmallVec::new(),
            output_schema,
            state_partition: StatePartitionId(id.0),
            node_type,
        };

        self.nodes.insert(id, node);
        self.name_index.insert(name, id);
        self.finalized = false;

        Ok(id)
    }

    /// Adds an edge between two nodes.
    ///
    /// # Errors
    ///
    /// Returns `DagError::NodeNotFound` if either node does not exist.
    /// Returns `DagError::CycleDetected` if the edge would create a self-loop.
    pub fn add_edge(&mut self, source: NodeId, target: NodeId) -> Result<EdgeId, DagError> {
        // Self-loop check
        if source == target {
            let name = self.node_name(source).unwrap_or_default();
            return Err(DagError::CycleDetected(name));
        }

        if !self.nodes.contains_key(&source) {
            return Err(DagError::NodeNotFound(format!("{source}")));
        }
        if !self.nodes.contains_key(&target) {
            return Err(DagError::NodeNotFound(format!("{target}")));
        }

        let id = EdgeId(self.next_edge_id);
        self.next_edge_id += 1;

        // Port indices are bounded by MAX_FAN_OUT (8), so truncation is safe.
        #[allow(clippy::cast_possible_truncation)]
        let source_port = self.nodes.get(&source).map_or(0, |n| n.outputs.len() as u8);
        #[allow(clippy::cast_possible_truncation)]
        let target_port = self.nodes.get(&target).map_or(0, |n| n.inputs.len() as u8);

        let edge = DagEdge {
            id,
            source,
            target,
            channel_type: DagChannelType::Spsc, // Derived during finalize
            partitioning: PartitioningStrategy::default(),
            source_port,
            target_port,
        };

        self.edges.insert(id, edge);

        // Update node adjacency lists
        if let Some(node) = self.nodes.get_mut(&source) {
            node.outputs.push(id);
        }
        if let Some(node) = self.nodes.get_mut(&target) {
            node.inputs.push(id);
        }

        self.finalized = false;

        Ok(id)
    }

    /// Finalizes the DAG: validates topology, computes execution order,
    /// derives channel types, and identifies shared stages.
    ///
    /// # Errors
    ///
    /// Returns `DagError::EmptyDag` if the DAG has no nodes.
    /// Returns `DagError::CycleDetected` if the graph contains cycles.
    /// Returns `DagError::DisconnectedNode` if a non-source, non-sink node
    /// has no inputs or no outputs.
    /// Returns `DagError::FanOutLimitExceeded` if any node exceeds the
    /// maximum fan-out limit.
    pub fn finalize(&mut self) -> Result<(), DagError> {
        if self.nodes.is_empty() {
            return Err(DagError::EmptyDag);
        }

        self.check_fan_out_limits()?;
        self.compute_execution_order()?;
        self.check_connected()?;
        self.derive_channel_types();
        self.identify_shared_stages();
        self.classify_source_sink_nodes();
        self.finalized = true;

        Ok(())
    }

    /// Validates the DAG topology without modifying internal state.
    ///
    /// # Errors
    ///
    /// Returns errors for cycles, disconnected nodes, or schema mismatches.
    pub fn validate(&self) -> Result<(), DagError> {
        if self.nodes.is_empty() {
            return Err(DagError::EmptyDag);
        }
        self.check_fan_out_limits()?;
        self.check_acyclic()?;
        self.check_connected()?;
        self.check_schemas()?;
        Ok(())
    }

    // ---- Accessors ----

    /// Returns the number of nodes in the DAG.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the number of edges in the DAG.
    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Returns a reference to a node by ID.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&DagNode> {
        self.nodes.get(&id)
    }

    /// Returns a reference to an edge by ID.
    #[must_use]
    pub fn edge(&self, id: EdgeId) -> Option<&DagEdge> {
        self.edges.get(&id)
    }

    /// Returns all nodes.
    #[must_use]
    pub fn nodes(&self) -> &FxHashMap<NodeId, DagNode> {
        &self.nodes
    }

    /// Returns all edges.
    #[must_use]
    pub fn edges(&self) -> &FxHashMap<EdgeId, DagEdge> {
        &self.edges
    }

    /// Returns the `NodeId` for a given node name.
    #[must_use]
    pub fn node_id_by_name(&self, name: &str) -> Option<NodeId> {
        self.name_index.get(name).copied()
    }

    /// Returns the node name for a given `NodeId`.
    #[must_use]
    pub fn node_name(&self, id: NodeId) -> Option<String> {
        self.nodes.get(&id).map(|n| n.name.clone())
    }

    /// Returns the number of outgoing edges from a node.
    #[inline]
    #[must_use]
    pub fn outgoing_edge_count(&self, node: NodeId) -> usize {
        self.nodes.get(&node).map_or(0, |n| n.outputs.len())
    }

    /// Returns the number of incoming edges to a node.
    #[inline]
    #[must_use]
    pub fn incoming_edge_count(&self, node: NodeId) -> usize {
        self.nodes.get(&node).map_or(0, |n| n.inputs.len())
    }

    /// Returns all source nodes (nodes with no inputs).
    #[must_use]
    pub fn sources(&self) -> &[NodeId] {
        &self.source_nodes
    }

    /// Returns all sink nodes (nodes with no outputs).
    #[must_use]
    pub fn sinks(&self) -> &[NodeId] {
        &self.sink_nodes
    }

    /// Returns nodes in topological execution order (dependencies first).
    #[must_use]
    pub fn execution_order(&self) -> &[NodeId] {
        &self.execution_order
    }

    /// Returns metadata for shared stages (nodes with fan-out > 1).
    #[must_use]
    pub fn shared_stages(&self) -> &FxHashMap<NodeId, SharedStageMetadata> {
        &self.shared_stages
    }

    /// Returns whether the DAG has been finalized.
    #[must_use]
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    // ---- Internal validation methods ----

    /// Checks that no node exceeds the maximum fan-out limit.
    fn check_fan_out_limits(&self) -> Result<(), DagError> {
        for node in self.nodes.values() {
            if node.outputs.len() > MAX_FAN_OUT {
                return Err(DagError::FanOutLimitExceeded {
                    node: node.name.clone(),
                    count: node.outputs.len(),
                    max: MAX_FAN_OUT,
                });
            }
        }
        Ok(())
    }

    /// Checks that the graph is acyclic using Kahn's algorithm.
    ///
    /// If the number of nodes in the topological order is less than
    /// the total number of nodes, a cycle exists.
    fn check_acyclic(&self) -> Result<(), DagError> {
        let (order, _) = self.kahn_topo_sort();
        if order.len() < self.nodes.len() {
            // Find a node involved in the cycle (not in the order)
            let ordered_set: FxHashSet<NodeId> = order.into_iter().collect();
            for node in self.nodes.values() {
                if !ordered_set.contains(&node.id) {
                    return Err(DagError::CycleDetected(node.name.clone()));
                }
            }
            return Err(DagError::CycleDetected("unknown".to_string()));
        }
        Ok(())
    }

    /// Checks that all non-source, non-sink nodes have both inputs and outputs.
    fn check_connected(&self) -> Result<(), DagError> {
        for node in self.nodes.values() {
            match node.node_type {
                DagNodeType::Source => {
                    if node.outputs.is_empty() {
                        return Err(DagError::DisconnectedNode(node.name.clone()));
                    }
                }
                DagNodeType::Sink => {
                    if node.inputs.is_empty() {
                        return Err(DagError::DisconnectedNode(node.name.clone()));
                    }
                }
                _ => {
                    if node.inputs.is_empty() && node.outputs.is_empty() {
                        return Err(DagError::DisconnectedNode(node.name.clone()));
                    }
                }
            }
        }
        Ok(())
    }

    /// Validates schema compatibility for connected edges.
    ///
    /// Source node schema must be compatible with the target node.
    /// For now, checks that field count and types match.
    fn check_schemas(&self) -> Result<(), DagError> {
        for edge in self.edges.values() {
            let source_node = self.nodes.get(&edge.source);
            let target_node = self.nodes.get(&edge.target);

            if let (Some(source), Some(target)) = (source_node, target_node) {
                let source_schema = &source.output_schema;
                let target_schema = &target.output_schema;

                // Empty schemas are compatible with anything (type-erased).
                if source_schema.fields().is_empty() || target_schema.fields().is_empty() {
                    continue;
                }

                // Check field count and types
                if source_schema.fields().len() != target_schema.fields().len() {
                    return Err(DagError::SchemaMismatch {
                        source_node: source.name.clone(),
                        target_node: target.name.clone(),
                        reason: format!(
                            "field count mismatch: {} vs {}",
                            source_schema.fields().len(),
                            target_schema.fields().len()
                        ),
                    });
                }

                for (sf, tf) in source_schema
                    .fields()
                    .iter()
                    .zip(target_schema.fields().iter())
                {
                    if sf.data_type() != tf.data_type() {
                        return Err(DagError::SchemaMismatch {
                            source_node: source.name.clone(),
                            target_node: target.name.clone(),
                            reason: format!(
                                "type mismatch for field '{}': {:?} vs '{}':{:?}",
                                sf.name(),
                                sf.data_type(),
                                tf.name(),
                                tf.data_type()
                            ),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Computes topological execution order using Kahn's algorithm.
    ///
    /// Also detects cycles: if the resulting order has fewer nodes
    /// than the DAG, a cycle exists.
    fn compute_execution_order(&mut self) -> Result<(), DagError> {
        let (order, processed) = self.kahn_topo_sort();
        if processed < self.nodes.len() {
            let ordered_set: FxHashSet<NodeId> = order.iter().copied().collect();
            for node in self.nodes.values() {
                if !ordered_set.contains(&node.id) {
                    return Err(DagError::CycleDetected(node.name.clone()));
                }
            }
            return Err(DagError::CycleDetected("unknown".to_string()));
        }
        self.execution_order = order;
        Ok(())
    }

    /// Kahn's algorithm for topological sort.
    ///
    /// Returns `(ordered_node_ids, count_of_processed_nodes)`.
    fn kahn_topo_sort(&self) -> (Vec<NodeId>, usize) {
        // Compute in-degree for each node
        let mut in_degree: FxHashMap<NodeId, usize> = FxHashMap::default();
        for node in self.nodes.values() {
            in_degree.entry(node.id).or_insert(0);
        }
        for edge in self.edges.values() {
            *in_degree.entry(edge.target).or_insert(0) += 1;
        }

        // Start with all nodes that have in-degree 0
        let mut queue: VecDeque<NodeId> = VecDeque::new();
        for (&node_id, &deg) in &in_degree {
            if deg == 0 {
                queue.push_back(node_id);
            }
        }

        // Sort initial queue by NodeId for deterministic ordering
        let mut initial: Vec<NodeId> = queue.drain(..).collect();
        initial.sort_by_key(|n| n.0);
        for id in initial {
            queue.push_back(id);
        }

        let mut order = Vec::with_capacity(self.nodes.len());
        let mut processed = 0;

        while let Some(node_id) = queue.pop_front() {
            order.push(node_id);
            processed += 1;

            // Collect and sort successors for deterministic ordering
            if let Some(node) = self.nodes.get(&node_id) {
                let mut successors: Vec<NodeId> = Vec::new();
                for &edge_id in &node.outputs {
                    if let Some(edge) = self.edges.get(&edge_id) {
                        let target = edge.target;
                        if let Some(deg) = in_degree.get_mut(&target) {
                            *deg = deg.saturating_sub(1);
                            if *deg == 0 {
                                successors.push(target);
                            }
                        }
                    }
                }
                successors.sort_by_key(|n| n.0);
                queue.extend(successors);
            }
        }

        (order, processed)
    }

    /// Derives channel types from the topology structure.
    ///
    /// - SPSC: source has 1 output, target has 1 input
    /// - SPMC: source has >1 outputs (fan-out)
    /// - MPSC: target has >1 inputs (fan-in)
    fn derive_channel_types(&mut self) {
        let edge_ids: Vec<EdgeId> = self.edges.keys().copied().collect();

        for edge_id in edge_ids {
            let (source_fan_out, target_fan_in) = {
                let edge = &self.edges[&edge_id];
                (
                    self.outgoing_edge_count(edge.source),
                    self.incoming_edge_count(edge.target),
                )
            };

            let channel_type = match (target_fan_in > 1, source_fan_out > 1) {
                (false, false) => DagChannelType::Spsc,
                (false, true) => DagChannelType::Spmc,
                // MPSC when target has multiple inputs (fan-in), regardless of
                // whether source also fans out. Each edge is MPSC from the
                // target's perspective.
                (true, _) => DagChannelType::Mpsc,
            };

            if let Some(edge) = self.edges.get_mut(&edge_id) {
                edge.channel_type = channel_type;
            }
        }
    }

    /// Identifies shared intermediate stages (nodes with fan-out > 1).
    fn identify_shared_stages(&mut self) {
        self.shared_stages.clear();

        for node in self.nodes.values() {
            if node.outputs.len() > 1 {
                let consumer_nodes: Vec<NodeId> = node
                    .outputs
                    .iter()
                    .filter_map(|&edge_id| self.edges.get(&edge_id).map(|e| e.target))
                    .collect();

                self.shared_stages.insert(
                    node.id,
                    SharedStageMetadata {
                        producer_node: node.id,
                        consumer_count: consumer_nodes.len(),
                        consumer_nodes,
                    },
                );
            }
        }
    }

    /// Classifies source and sink nodes based on connectivity.
    fn classify_source_sink_nodes(&mut self) {
        self.source_nodes.clear();
        self.sink_nodes.clear();

        for node in self.nodes.values() {
            if node.inputs.is_empty() {
                self.source_nodes.push(node.id);
            }
            if node.outputs.is_empty() {
                self.sink_nodes.push(node.id);
            }
        }

        // Sort for deterministic ordering
        self.source_nodes.sort_by_key(|n| n.0);
        self.sink_nodes.sort_by_key(|n| n.0);
    }
}

impl StreamingDag {
    /// Constructs a DAG from an `MvRegistry` and base table schemas.
    ///
    /// Creates a Source node for each base table and a `MaterializedView` node
    /// for each MV, then connects them according to the registry's dependency
    /// graph. The resulting DAG is finalized and ready for execution.
    ///
    /// # Arguments
    ///
    /// * `registry` - The MV registry with dependency information
    /// * `base_table_schemas` - Schemas for each base table referenced by MVs
    ///
    /// # Errors
    ///
    /// Returns `DagError::EmptyDag` if the registry is empty and has no base tables.
    /// Returns `DagError::BaseTableSchemaNotFound` if a base table schema is missing.
    pub fn from_mv_registry(
        registry: &crate::mv::MvRegistry,
        base_table_schemas: &FxHashMap<String, SchemaRef>,
    ) -> Result<Self, DagError> {
        if registry.is_empty() && registry.base_tables().is_empty() {
            return Err(DagError::EmptyDag);
        }

        let mut dag = Self::new();

        // Add source nodes for base tables that are actually referenced by MVs
        for base_table in registry.base_tables() {
            let schema = base_table_schemas
                .get(base_table)
                .ok_or_else(|| DagError::BaseTableSchemaNotFound(base_table.clone()))?;
            dag.add_node(base_table, DagNodeType::Source, schema.clone())?;
        }

        // Add MV nodes in topological order
        for mv_name in registry.topo_order() {
            let mv = registry
                .get(mv_name)
                .ok_or_else(|| DagError::NodeNotFound(mv_name.clone()))?;
            dag.add_node(mv_name, DagNodeType::MaterializedView, mv.schema.clone())?;
        }

        // Connect edges: for each MV, connect its sources to it
        for mv_name in registry.topo_order() {
            let mv = registry
                .get(mv_name)
                .ok_or_else(|| DagError::NodeNotFound(mv_name.clone()))?;
            let target_id = dag
                .node_id_by_name(mv_name)
                .ok_or_else(|| DagError::NodeNotFound(mv_name.clone()))?;
            for source_name in &mv.sources {
                let source_id = dag
                    .node_id_by_name(source_name)
                    .ok_or_else(|| DagError::NodeNotFound(source_name.clone()))?;
                dag.add_edge(source_id, target_id)?;
            }
        }

        dag.finalize()?;
        Ok(dag)
    }
}

impl Default for StreamingDag {
    fn default() -> Self {
        Self::new()
    }
}
