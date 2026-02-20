//! DAG builder API for programmatic topology construction.
//!
//! Provides `DagBuilder` for fluent DAG construction in Ring 2 and
//! `FanOutBuilder` for creating fan-out branches from shared stages.

use arrow_schema::SchemaRef;
use rustc_hash::FxHashMap;

use super::error::DagError;
use super::topology::{DagNodeType, StreamingDag};

/// Fluent builder for constructing `StreamingDag` topologies.
///
/// Constructed in Ring 2. Once `build()` is called, the topology is
/// immutable and can be executed in Ring 0.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::dag::DagBuilder;
///
/// let dag = DagBuilder::new()
///     .source("trades", schema.clone())
///     .operator("dedup", schema.clone())
///     .operator("normalize", schema.clone())
///     .connect("trades", "dedup")
///     .connect("dedup", "normalize")
///     .fan_out("normalize", |b| {
///         b.branch("vwap", schema.clone())
///          .branch("anomaly", schema.clone())
///          .branch("position", schema.clone())
///     })
///     .sink_for("vwap", "analytics_sink", schema.clone())
///     .sink_for("anomaly", "alert_sink", schema.clone())
///     .sink_for("position", "risk_sink", schema.clone())
///     .build()?;
/// ```
pub struct DagBuilder {
    /// Nodes being constructed.
    nodes: Vec<(String, DagNodeType, SchemaRef)>,
    /// Edges being constructed.
    edges: Vec<(String, String)>,
    /// Name -> index mapping for duplicate detection.
    name_index: FxHashMap<String, usize>,
}

impl DagBuilder {
    /// Creates a new DAG builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            name_index: FxHashMap::default(),
        }
    }

    /// Adds a source node to the DAG.
    ///
    /// Source nodes are entry points that receive data from external
    /// systems via the Connector SDK.
    #[must_use]
    pub fn source(mut self, name: &str, schema: SchemaRef) -> Self {
        let idx = self.nodes.len();
        self.nodes
            .push((name.to_string(), DagNodeType::Source, schema));
        self.name_index.insert(name.to_string(), idx);
        self
    }

    /// Adds a stateful operator node to the DAG.
    #[must_use]
    pub fn operator(mut self, name: &str, schema: SchemaRef) -> Self {
        let idx = self.nodes.len();
        self.nodes
            .push((name.to_string(), DagNodeType::StatefulOperator, schema));
        self.name_index.insert(name.to_string(), idx);
        self
    }

    /// Adds a stateless operator node to the DAG.
    #[must_use]
    pub fn stateless_operator(mut self, name: &str, schema: SchemaRef) -> Self {
        let idx = self.nodes.len();
        self.nodes
            .push((name.to_string(), DagNodeType::StatelessOperator, schema));
        self.name_index.insert(name.to_string(), idx);
        self
    }

    /// Adds a materialized view node to the DAG.
    #[must_use]
    pub fn materialized_view(mut self, name: &str, schema: SchemaRef) -> Self {
        let idx = self.nodes.len();
        self.nodes
            .push((name.to_string(), DagNodeType::MaterializedView, schema));
        self.name_index.insert(name.to_string(), idx);
        self
    }

    /// Connects two nodes with an edge.
    #[must_use]
    pub fn connect(mut self, from: &str, to: &str) -> Self {
        self.edges.push((from.to_string(), to.to_string()));
        self
    }

    /// Creates a fan-out from a shared stage to multiple branches.
    ///
    /// The `branches` closure receives a `FanOutBuilder` that allows
    /// adding multiple downstream branches from the shared node.
    /// Each branch is automatically connected to the shared node.
    #[must_use]
    pub fn fan_out<F>(mut self, shared_node: &str, branches: F) -> Self
    where
        F: FnOnce(FanOutBuilder) -> FanOutBuilder,
    {
        let fan_out = branches(FanOutBuilder::new(shared_node.to_string()));
        for (branch_name, branch_type, branch_schema) in fan_out.branches {
            let idx = self.nodes.len();
            self.nodes
                .push((branch_name.clone(), branch_type, branch_schema));
            self.name_index.insert(branch_name.clone(), idx);
            self.edges.push((shared_node.to_string(), branch_name));
        }
        self
    }

    /// Adds a sink node and connects it to an upstream node.
    #[must_use]
    pub fn sink_for(mut self, upstream: &str, sink_name: &str, schema: SchemaRef) -> Self {
        let idx = self.nodes.len();
        self.nodes
            .push((sink_name.to_string(), DagNodeType::Sink, schema));
        self.name_index.insert(sink_name.to_string(), idx);
        self.edges
            .push((upstream.to_string(), sink_name.to_string()));
        self
    }

    /// Adds a sink node to the DAG (without auto-connecting).
    ///
    /// Use `connect()` to manually wire upstream nodes.
    #[must_use]
    pub fn sink(mut self, name: &str, schema: SchemaRef) -> Self {
        let idx = self.nodes.len();
        self.nodes
            .push((name.to_string(), DagNodeType::Sink, schema));
        self.name_index.insert(name.to_string(), idx);
        self
    }

    /// Builds the immutable DAG topology.
    ///
    /// Validates the topology (acyclic, connected, schema-compatible),
    /// computes topological order, derives channel types, and identifies
    /// shared stages.
    ///
    /// # Errors
    ///
    /// Returns `DagError::CycleDetected` if the graph contains cycles.
    /// Returns `DagError::DisconnectedNode` if a node has no connections.
    /// Returns `DagError::NodeNotFound` if an edge references a missing node.
    /// Returns `DagError::DuplicateNode` if nodes have duplicate names.
    /// Returns `DagError::EmptyDag` if no nodes were added.
    pub fn build(self) -> Result<StreamingDag, DagError> {
        let mut dag = StreamingDag::new();

        // Add nodes
        for (name, node_type, schema) in self.nodes {
            dag.add_node(name, node_type, schema)?;
        }

        // Add edges
        for (from, to) in self.edges {
            let from_id = dag
                .node_id_by_name(&from)
                .ok_or_else(|| DagError::NodeNotFound(from.clone()))?;
            let to_id = dag
                .node_id_by_name(&to)
                .ok_or_else(|| DagError::NodeNotFound(to.clone()))?;
            dag.add_edge(from_id, to_id)?;
        }

        // Validate and finalize
        dag.finalize()?;

        Ok(dag)
    }
}

impl Default for DagBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for fan-out branches from a shared stage.
pub struct FanOutBuilder {
    #[allow(dead_code)] // Stored for future DAG visualization and error messages
    shared_node: String,
    branches: Vec<(String, DagNodeType, SchemaRef)>,
}

impl FanOutBuilder {
    fn new(shared_node: String) -> Self {
        Self {
            shared_node,
            branches: Vec::new(),
        }
    }

    /// Adds a stateful operator branch from the shared stage.
    #[must_use]
    pub fn branch(mut self, name: &str, schema: SchemaRef) -> Self {
        self.branches
            .push((name.to_string(), DagNodeType::StatefulOperator, schema));
        self
    }

    /// Adds a stateless operator branch from the shared stage.
    #[must_use]
    pub fn stateless_branch(mut self, name: &str, schema: SchemaRef) -> Self {
        self.branches
            .push((name.to_string(), DagNodeType::StatelessOperator, schema));
        self
    }
}
