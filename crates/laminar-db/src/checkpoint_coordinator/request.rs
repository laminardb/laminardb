use std::collections::HashMap;
#[cfg(feature = "cluster")]
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use laminar_core::checkpoint::{ChannelProgress, ConnectorCheckpoint, StateFrameKey};

#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    pub checkpoint_timeout: Duration,
    pub(crate) cleanup_timeout: Duration,
    pub(crate) quorum_timeout: Duration,
    pub max_node_data_bytes: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_timeout: Duration::from_secs(120),
            cleanup_timeout: Duration::from_secs(30),
            quorum_timeout: Duration::from_secs(3),
            max_node_data_bytes:
                laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CheckpointRequest {
    pub flags: u64,
    pub handoff_replay_pending: bool,
    /// Capture-time proof that this cut can be restored under a different vnode assignment.
    pub reassignment_portable: bool,
    pub assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
    pub state_frames: Vec<CapturedStateFrame>,
    pub(crate) managed_vnode_operators: Vec<ManagedVnodeOperator>,
    pub source_names: Vec<String>,
    pub channel_progress: Vec<ChannelProgress>,
    pub source_offset_overrides: HashMap<String, ConnectorCheckpoint>,
    #[cfg(feature = "cluster")]
    pub(crate) subscription_output:
        Option<Arc<crate::subscription::cluster::PreparedNodeSubscriptionOutput>>,
}

#[derive(Debug, Clone)]
pub struct CapturedStateFrame {
    pub key: StateFrameKey,
    pub state: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManagedVnodePlacement {
    GlobalSingleton,
    VnodeKeyed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedVnodeOperator {
    pub(crate) operator_id: String,
    pub(crate) placement: ManagedVnodePlacement,
}
