use std::collections::HashMap;
#[cfg(feature = "cluster")]
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use laminar_core::checkpoint::{
    ChannelProgress, ConnectorCheckpoint, EventTimeBounds, StateFrameKey,
};

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
    pub(crate) view_logs: Vec<CapturedViewLog>,
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

/// One append-only view's log at the cut.
#[derive(Debug, Clone)]
pub(crate) struct CapturedViewLog {
    pub(crate) view: String,
    pub(crate) next_sequence: u64,
    pub(crate) frontier: Option<i64>,
    pub(crate) idle: bool,
    pub(crate) segments: Vec<CapturedViewLogSegment>,
}

#[derive(Debug, Clone)]
pub(crate) enum CapturedViewLogSegment {
    /// Rows appended since the previous capture.
    New {
        partition: u16,
        sequence: u64,
        rows: u64,
        event_time: Option<EventTimeBounds>,
        payload: Bytes,
    },
    /// A segment the committed predecessor holds and retention keeps.
    Retained { partition: u16, sequence: u64 },
}

impl CapturedViewLogSegment {
    pub(crate) const fn position(&self) -> (u16, u64) {
        match self {
            Self::New {
                partition,
                sequence,
                ..
            }
            | Self::Retained {
                partition,
                sequence,
            } => (*partition, *sequence),
        }
    }
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
