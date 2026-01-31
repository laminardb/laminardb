//! Error types for DAG topology operations.

/// Errors that can occur during DAG construction and validation.
#[derive(Debug, thiserror::Error)]
pub enum DagError {
    /// The DAG contains a cycle involving the named node.
    #[error("cycle detected involving node: {0}")]
    CycleDetected(String),

    /// A node has no inputs and no outputs (and is not a source or sink).
    #[error("disconnected node: {0}")]
    DisconnectedNode(String),

    /// An edge references a node that does not exist.
    #[error("node not found: {0}")]
    NodeNotFound(String),

    /// A node with the same name already exists.
    #[error("duplicate node name: {0}")]
    DuplicateNode(String),

    /// Connected edges have incompatible schemas.
    #[error("schema mismatch between {source_node} and {target_node}: {reason}")]
    SchemaMismatch {
        /// Source node name.
        source_node: String,
        /// Target node name.
        target_node: String,
        /// Description of the incompatibility.
        reason: String,
    },

    /// A node exceeds the maximum fan-out limit.
    #[error("fan-out limit exceeded: node {node} has {count} outputs (max {max})")]
    FanOutLimitExceeded {
        /// Node name.
        node: String,
        /// Actual fan-out count.
        count: usize,
        /// Maximum allowed fan-out.
        max: usize,
    },

    /// The DAG is empty (no nodes).
    #[error("empty DAG: no nodes")]
    EmptyDag,

    /// A shared stage multicast buffer is full (backpressure).
    #[error("backpressure: buffer full")]
    BackpressureFull,

    /// A checkpoint barrier was triggered while another checkpoint is in progress.
    #[error("checkpoint already in progress: epoch {0}")]
    CheckpointInProgress(u64),

    /// Attempted to finalize a checkpoint when none is in progress.
    #[error("no checkpoint in progress")]
    NoCheckpointInProgress,

    /// Attempted to finalize a checkpoint before all nodes have reported.
    #[error("checkpoint incomplete: {pending} nodes still pending")]
    CheckpointIncomplete {
        /// Number of nodes that have not yet reported.
        pending: usize,
    },

    /// No checkpoint snapshots are available for recovery.
    #[error("no checkpoint snapshots available")]
    CheckpointNotFound,

    /// An operator failed to restore from a checkpoint snapshot.
    #[error("restore failed for node '{node_id}': {reason}")]
    RestoreFailed {
        /// The node that failed to restore.
        node_id: String,
        /// Description of the failure.
        reason: String,
    },

    /// A base table schema was not provided for DAG construction from `MvRegistry`.
    #[error("base table schema not found: {0}")]
    BaseTableSchemaNotFound(String),
}
