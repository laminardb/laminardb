//! Interval join operator for the `OperatorGraph`.
//!
//! Buffers left/right rows across cycles for
//! `right_ts BETWEEN left_ts AND left_ts + time_bound`; evicts on watermark advance.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use std::collections::BTreeMap;
#[cfg(feature = "cluster")]
use std::collections::VecDeque;

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

#[cfg(feature = "cluster")]
use laminar_core::state::NodeId;
use laminar_core::state::{KeyGroupCount, VnodeAssignmentSnapshot, VnodeRegistry, LOCAL_NODE_ID};
use laminar_sql::translator::StreamJoinConfig;

use crate::error::DbError;
use crate::interval_join::{
    execute_interval_join_cycle, join_type_tag, ArchivedJoinStateCheckpoint,
    IntervalJoinCheckpointCapture, IntervalJoinOutputBudget, IntervalJoinState,
    JoinStateCheckpoint, HEAP_ALLOCATION_CHARGE,
};
use crate::operator::ProjectingJoinState;
#[cfg(feature = "cluster")]
use crate::operator_graph::merge_input_frontier_iter;
use crate::operator_graph::{
    CapturedVnodeState, EncodedStateFrame, InputFrontier, StateFrameCapture,
};
use crate::operator_graph::{GraphOperator, ManagedStateAccountingSnapshot, OperatorCheckpoint};

#[cfg(feature = "cluster")]
use crate::operator::sql_query::ClusterShuffleConfig;
#[cfg(feature = "cluster")]
use crate::operator_graph::{ManagedVnodeTransition, ManagedVnodeTransitionMode};

const OPERATOR_CHECKPOINT_VERSION: u8 = 3;
const ABSENT_VNODE: u8 = 0;
const PRESENT_VNODE: u8 = 1;
const VNODE_FRAME_VERSION: u8 = 1;
const VNODE_FRAME_HEADER_LEN: usize = std::mem::align_of::<ArchivedJoinStateCheckpoint>();
const fn vnode_frame_header(tag: u8) -> [u8; VNODE_FRAME_HEADER_LEN] {
    let mut header = [0_u8; VNODE_FRAME_HEADER_LEN];
    header[0] = tag;
    header[1] = VNODE_FRAME_VERSION;
    header
}
const ABSENT_VNODE_FRAME: [u8; VNODE_FRAME_HEADER_LEN] = vnode_frame_header(ABSENT_VNODE);
#[cfg(feature = "cluster")]
const REMOTE_EVENT_CHARGE: usize = std::mem::size_of::<IntervalRemoteEvent>();
#[cfg(feature = "cluster")]
const PEER_CHANNEL_ENTRY_CHARGE: usize = 64;
#[cfg(feature = "cluster")]
const PENDING_ROUTE_ENTRY_CHARGE: usize = 64;
#[cfg(feature = "cluster")]
const RETAINED_BATCH_ARC_CHARGE: usize =
    std::mem::size_of::<crate::operator::RetainedBatch>() + 2 * std::mem::size_of::<usize>();
#[cfg(feature = "cluster")]
const ROW_VNODE_ARC_CHARGE: usize = 2 * std::mem::size_of::<usize>();

#[derive(Clone, Copy, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
enum JoinInputSide {
    Left,
    Right,
}

impl JoinInputSide {
    #[cfg(feature = "cluster")]
    const fn port(self) -> usize {
        match self {
            Self::Left => 0,
            Self::Right => 1,
        }
    }

    #[cfg(feature = "cluster")]
    const fn name(self) -> &'static str {
        match self {
            Self::Left => "left",
            Self::Right => "right",
        }
    }
}

#[derive(Clone, Copy, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct IntervalCheckpointFrontier {
    watermark: Option<i64>,
    idle: bool,
}

impl From<InputFrontier> for IntervalCheckpointFrontier {
    fn from(frontier: InputFrontier) -> Self {
        Self {
            watermark: frontier.watermark,
            idle: frontier.idle,
        }
    }
}

impl From<IntervalCheckpointFrontier> for InputFrontier {
    fn from(frontier: IntervalCheckpointFrontier) -> Self {
        Self {
            watermark: frontier.watermark,
            idle: frontier.idle,
        }
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
enum IntervalCheckpointEvent {
    Data {
        recovery_gen: u64,
        routed_vnodes: Vec<u32>,
        ipc: Vec<u8>,
    },
    Frontier {
        recovery_gen: u64,
        frontier: IntervalCheckpointFrontier,
    },
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct IntervalCheckpointChannel {
    peer: u64,
    applied: IntervalCheckpointFrontier,
    events: Vec<IntervalCheckpointEvent>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct IntervalClusterCheckpoint {
    assignment_version: u64,
    owner_map_digest: [u8; 32],
    self_id: u64,
    recovery_gen: u64,
    local_frontiers: [IntervalCheckpointFrontier; 2],
    remote_side_cursor: u8,
    remote_peer_cursors: [Option<u64>; 2],
    channels: [Vec<IntervalCheckpointChannel>; 2],
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct IntervalJoinOperatorCheckpoint {
    version: u8,
    join_type: u8,
    left_keys: Vec<String>,
    right_keys: Vec<String>,
    left_time_column: String,
    right_time_column: String,
    left_table: String,
    right_table: String,
    bound_ms: i64,
    applied_left_watermark: i64,
    applied_right_watermark: i64,
    applied_left_idle: bool,
    applied_right_idle: bool,
    cluster: Option<IntervalClusterCheckpoint>,
}

#[cfg(feature = "cluster")]
enum CapturedIntervalEvent {
    Data {
        recovery_gen: u64,
        retained: Arc<crate::operator::RetainedBatch>,
    },
    Frontier {
        recovery_gen: u64,
        frontier: InputFrontier,
    },
}

#[cfg(feature = "cluster")]
struct CapturedIntervalChannel {
    peer: u64,
    applied: InputFrontier,
    events: Vec<CapturedIntervalEvent>,
}

#[cfg(feature = "cluster")]
struct CapturedIntervalCluster {
    assignment_version: u64,
    owner_map_digest: [u8; 32],
    self_id: u64,
    recovery_gen: u64,
    local_frontiers: [InputFrontier; 2],
    remote_side_cursor: u8,
    remote_peer_cursors: [Option<u64>; 2],
    channels: [Vec<CapturedIntervalChannel>; 2],
}

struct IntervalJoinOperatorCheckpointCapture {
    checkpoint: IntervalJoinOperatorCheckpoint,
    #[cfg(feature = "cluster")]
    cluster: Option<CapturedIntervalCluster>,
    retained_bytes: u64,
}

impl IntervalJoinOperatorCheckpointCapture {
    const fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }

    fn calculate_retained_bytes_for(
        left_keys_capacity: usize,
        right_keys_capacity: usize,
        string_capacities: impl IntoIterator<Item = usize>,
    ) -> Result<u64, DbError> {
        fn allocation(bytes: usize) -> Result<usize, DbError> {
            bytes
                .checked_add(if bytes == 0 {
                    0
                } else {
                    HEAP_ALLOCATION_CHARGE
                })
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "interval join whole-state capture accounting overflow".into(),
                    )
                })
        }

        fn roster<T>(capacity: usize) -> Result<usize, DbError> {
            allocation(
                capacity
                    .checked_mul(std::mem::size_of::<T>())
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join whole-state roster accounting overflow".into(),
                        )
                    })?,
            )
        }

        fn add(total: &mut usize, bytes: usize) -> Result<(), DbError> {
            *total = total.checked_add(bytes).ok_or_else(|| {
                DbError::Checkpoint("interval join whole-state capture accounting overflow".into())
            })?;
            Ok(())
        }

        let mut bytes = std::mem::size_of::<Self>();
        add(&mut bytes, roster::<String>(left_keys_capacity)?)?;
        add(&mut bytes, roster::<String>(right_keys_capacity)?)?;
        for capacity in string_capacities {
            add(&mut bytes, allocation(capacity)?)?;
        }

        u64::try_from(bytes).map_err(|_| {
            DbError::Checkpoint("interval join whole-state capture exceeds u64".into())
        })
    }

    fn calculate_retained_bytes(&self) -> Result<u64, DbError> {
        Self::calculate_retained_bytes_for(
            self.checkpoint.left_keys.capacity(),
            self.checkpoint.right_keys.capacity(),
            self.checkpoint
                .left_keys
                .iter()
                .chain(&self.checkpoint.right_keys)
                .chain([
                    &self.checkpoint.left_time_column,
                    &self.checkpoint.right_time_column,
                    &self.checkpoint.left_table,
                    &self.checkpoint.right_table,
                ])
                .map(String::capacity),
        )
        .and_then(|base| {
            #[cfg(feature = "cluster")]
            let extra: Result<usize, DbError> =
                self.cluster.as_ref().map_or(Ok(0usize), |cluster| {
                    let allocation = |bytes: usize| {
                        bytes.checked_add(usize::from(bytes != 0) * HEAP_ALLOCATION_CHARGE)
                    };
                    let mut bytes = 0usize;
                    for channels in &cluster.channels {
                        bytes = bytes
                            .checked_add(
                                allocation(
                                    channels
                                        .capacity()
                                        .checked_mul(std::mem::size_of::<CapturedIntervalChannel>())
                                        .ok_or_else(|| {
                                            DbError::Checkpoint(
                                                "interval join channel capture accounting overflow"
                                                    .into(),
                                            )
                                        })?,
                                )
                                .ok_or_else(|| {
                                    DbError::Checkpoint(
                                        "interval join channel capture accounting overflow".into(),
                                    )
                                })?,
                            )
                            .ok_or_else(|| {
                                DbError::Checkpoint(
                                    "interval join channel capture accounting overflow".into(),
                                )
                            })?;
                        for channel in channels {
                            bytes = bytes
                                .checked_add(
                                    allocation(
                                        channel
                                            .events
                                            .capacity()
                                            .checked_mul(
                                                std::mem::size_of::<CapturedIntervalEvent>(),
                                            )
                                            .ok_or_else(|| {
                                                DbError::Checkpoint(
                                                "interval join event capture accounting overflow"
                                                    .into(),
                                            )
                                            })?,
                                    )
                                    .ok_or_else(|| {
                                        DbError::Checkpoint(
                                            "interval join event capture accounting overflow"
                                                .into(),
                                        )
                                    })?,
                                )
                                .ok_or_else(|| {
                                    DbError::Checkpoint(
                                        "interval join event capture accounting overflow".into(),
                                    )
                                })?;
                            for event in &channel.events {
                                if let CapturedIntervalEvent::Data { retained, .. } = event {
                                    bytes = bytes
                                        .checked_add(retained.heap_bytes().ok_or_else(|| {
                                            DbError::Checkpoint(
                                            "interval join retained shuffle accounting overflow"
                                                .into(),
                                        )
                                        })?)
                                        .and_then(|bytes| {
                                            bytes.checked_add(RETAINED_BATCH_ARC_CHARGE)
                                        })
                                        .ok_or_else(|| {
                                            DbError::Checkpoint(
                                            "interval join retained shuffle accounting overflow"
                                                .into(),
                                        )
                                        })?;
                                }
                            }
                        }
                    }
                    Ok(bytes)
                });
            #[cfg(not(feature = "cluster"))]
            let extra: Result<usize, DbError> = Ok(0);
            let extra = u64::try_from(extra?).map_err(|_| {
                DbError::Checkpoint("interval join channel capture exceeds u64".into())
            })?;
            base.checked_add(extra).ok_or_else(|| {
                DbError::Checkpoint("interval join whole-state capture accounting overflow".into())
            })
        })
    }

    fn encode(self, max_working_bytes: usize, context: &str) -> Result<Vec<u8>, DbError> {
        #[cfg(feature = "cluster")]
        let mut capture = self;
        #[cfg(not(feature = "cluster"))]
        let capture = self;
        #[cfg(feature = "cluster")]
        let mut remaining = max_working_bytes;
        #[cfg(not(feature = "cluster"))]
        let remaining = max_working_bytes;
        #[cfg(feature = "cluster")]
        if let Some(cluster) = capture.cluster.take() {
            let allocation = |capacity: usize, item_bytes: usize| {
                capacity
                    .checked_mul(item_bytes)
                    .and_then(|bytes| {
                        bytes.checked_add(usize::from(bytes != 0) * HEAP_ALLOCATION_CHARGE)
                    })
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!("{context}: channel scratch overflow"))
                    })
            };
            let mut encoded_channels = [Vec::new(), Vec::new()];
            for (port, encoded_port) in encoded_channels.iter_mut().enumerate() {
                encoded_port
                    .try_reserve_exact(cluster.channels[port].len())
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "{context}: channel roster cannot be reserved: {error}"
                        ))
                    })?;
                let channel_bytes = allocation(
                    encoded_port.capacity(),
                    std::mem::size_of::<IntervalCheckpointChannel>(),
                )?;
                remaining = remaining.checked_sub(channel_bytes).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "{context}: encoded channel roster requires {channel_bytes} bytes"
                    ))
                })?;
                for channel in &cluster.channels[port] {
                    let mut events = Vec::new();
                    events
                        .try_reserve_exact(channel.events.len())
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "{context}: peer {} event roster cannot be reserved: {error}",
                                channel.peer
                            ))
                        })?;
                    let event_bytes = allocation(
                        events.capacity(),
                        std::mem::size_of::<IntervalCheckpointEvent>(),
                    )?;
                    remaining = remaining.checked_sub(event_bytes).ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "{context}: peer {} encoded event roster requires {event_bytes} bytes",
                            channel.peer
                        ))
                    })?;
                    for event in &channel.events {
                        let encoded = match event {
                            CapturedIntervalEvent::Data {
                                recovery_gen,
                                retained,
                            } => {
                                let mut routed_vnodes = Vec::new();
                                routed_vnodes
                                    .try_reserve_exact(retained.routed_vnodes().len())
                                    .map_err(|error| {
                                        DbError::Checkpoint(format!(
                                            "{context}: peer {} route roster cannot be reserved: {error}",
                                            channel.peer
                                        ))
                                    })?;
                                let route_bytes = allocation(
                                    routed_vnodes.capacity(),
                                    std::mem::size_of::<u32>(),
                                )?;
                                remaining = remaining.checked_sub(route_bytes).ok_or_else(|| {
                                    DbError::Checkpoint(format!(
                                        "{context}: peer {} route roster requires {route_bytes} bytes",
                                        channel.peer
                                    ))
                                })?;
                                routed_vnodes.extend_from_slice(retained.routed_vnodes());
                                let ipc =
                                    laminar_core::serialization::serialize_batches_stream_bounded(
                                        retained.batch().schema().as_ref(),
                                        std::iter::once(retained.batch()),
                                        remaining,
                                    )
                                    .map_err(|error| {
                                        DbError::Checkpoint(format!(
                                            "{context}: peer {} queued data serialization: {error}",
                                            channel.peer
                                        ))
                                    })?;
                                remaining =
                                    remaining.checked_sub(ipc.capacity()).ok_or_else(|| {
                                        DbError::Checkpoint(format!(
                                            "{context}: queued data byte accounting overflow"
                                        ))
                                    })?;
                                IntervalCheckpointEvent::Data {
                                    recovery_gen: *recovery_gen,
                                    routed_vnodes,
                                    ipc,
                                }
                            }
                            CapturedIntervalEvent::Frontier {
                                recovery_gen,
                                frontier,
                            } => IntervalCheckpointEvent::Frontier {
                                recovery_gen: *recovery_gen,
                                frontier: (*frontier).into(),
                            },
                        };
                        events.push(encoded);
                    }
                    encoded_port.push(IntervalCheckpointChannel {
                        peer: channel.peer,
                        applied: channel.applied.into(),
                        events,
                    });
                }
            }
            capture.checkpoint.cluster = Some(IntervalClusterCheckpoint {
                assignment_version: cluster.assignment_version,
                owner_map_digest: cluster.owner_map_digest,
                self_id: cluster.self_id,
                recovery_gen: cluster.recovery_gen,
                local_frontiers: cluster.local_frontiers.map(Into::into),
                remote_side_cursor: cluster.remote_side_cursor,
                remote_peer_cursors: cluster.remote_peer_cursors,
                channels: encoded_channels,
            });
        }

        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(remaining),
        );
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&capture.checkpoint, writer)
            .map(|bytes| bytes.into_inner().into_vec())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "{context}: archive serialization exceeded the remaining {remaining}-byte cumulative budget: {error}"
                ))
            })
    }
}

#[derive(Clone, Copy)]
struct IntervalHandoffCut {
    left_watermark: i64,
    right_watermark: i64,
    #[cfg(feature = "cluster")]
    left_idle: bool,
    #[cfg(feature = "cluster")]
    right_idle: bool,
}

#[cfg(feature = "cluster")]
struct PreparedIntervalJoinTransition {
    replacements: Vec<(u32, Option<Box<IntervalJoinState>>)>,
    local_assignment: VnodeAssignmentSnapshot,
    resident_vnodes: Vec<u32>,
    cluster_peers: Arc<[u64]>,
    peer_channels: [BTreeMap<u64, IntervalPeerChannel>; 2],
    bootstrap_broadcast: bool,
    handoff_cut: Option<IntervalHandoffCut>,
}

#[cfg(feature = "cluster")]
enum IntervalJoinTransitionCleanup {
    Aborted(PreparedIntervalJoinTransition),
    Published(PreparedIntervalJoinTransition),
}

#[cfg(feature = "cluster")]
struct QueuedIntervalBatch {
    retained: Arc<crate::operator::RetainedBatch>,
    row_vnodes: Arc<[u32]>,
    charged_bytes: usize,
}

#[cfg(feature = "cluster")]
struct IntervalRemoteEvent {
    assignment_version: u64,
    recovery_gen: u64,
    payload: IntervalRemoteEventPayload,
}

#[cfg(feature = "cluster")]
enum IntervalRemoteEventPayload {
    Data(QueuedIntervalBatch),
    Frontier(InputFrontier),
}

#[cfg(feature = "cluster")]
impl IntervalRemoteEvent {
    fn payload_bytes(&self) -> usize {
        match &self.payload {
            IntervalRemoteEventPayload::Data(batch) => batch.charged_bytes,
            IntervalRemoteEventPayload::Frontier(_) => 0,
        }
    }
}

#[cfg(feature = "cluster")]
#[derive(Default)]
struct IntervalPeerChannel {
    applied: InputFrontier,
    accepted: InputFrontier,
    events: VecDeque<IntervalRemoteEvent>,
}

#[cfg(feature = "cluster")]
struct IntervalClusterInputPlan {
    routed: BTreeMap<u32, [Vec<RecordBatch>; 2]>,
    outbound: Vec<(u64, laminar_core::shuffle::ShuffleMessage)>,
    local_frontiers: [InputFrontier; 2],
    effective_frontiers: [InputFrontier; 2],
}

#[cfg(feature = "cluster")]
type IntervalSendOutcome = (
    Result<(), DbError>,
    Option<Vec<(u64, laminar_core::shuffle::ShuffleMessage)>>,
);

#[cfg(feature = "cluster")]
type IntervalSendTask = tokio::task::JoinHandle<()>;

#[cfg(feature = "cluster")]
struct PendingIntervalClusterInput {
    routed: BTreeMap<u32, [Vec<RecordBatch>; 2]>,
    outbound: Option<Vec<(u64, laminar_core::shuffle::ShuffleMessage)>>,
    local_frontiers: [InputFrontier; 2],
    send: Option<IntervalSendTask>,
    outcome: Option<tokio::sync::oneshot::Receiver<IntervalSendOutcome>>,
    accounted_bytes: usize,
}

#[cfg(feature = "cluster")]
impl Drop for PendingIntervalClusterInput {
    fn drop(&mut self) {
        if let Some(send) = &self.send {
            send.abort();
        }
    }
}

#[cfg(feature = "cluster")]
enum PendingIntervalCompletion {
    Waiting,
    RetryLater,
    Applied(Vec<RecordBatch>),
}

pub(crate) struct IntervalJoinOperator {
    config: StreamJoinConfig,
    key_group_count: KeyGroupCount,
    local_assignment: VnodeAssignmentSnapshot,
    vnode_states: Vec<Option<Box<IntervalJoinState>>>,
    resident_vnodes: Vec<u32>,
    dirty_vnodes: Vec<bool>,
    dirty_vnode_roster: Vec<u32>,
    full_vnode_capture_required: bool,
    max_managed_state_bytes: usize,
    input_schemas: Option<(SchemaRef, SchemaRef)>,
    projection: ProjectingJoinState,
    #[cfg(feature = "cluster")]
    cluster_shuffle: Option<ClusterShuffleConfig>,
    #[cfg(feature = "cluster")]
    cluster_peers: Arc<[u64]>,
    #[cfg(feature = "cluster")]
    local_frontiers: [InputFrontier; 2],
    #[cfg(feature = "cluster")]
    peer_channels: [BTreeMap<u64, IntervalPeerChannel>; 2],
    #[cfg(feature = "cluster")]
    last_broadcasts: [InputFrontier; 2],
    #[cfg(feature = "cluster")]
    remote_side_cursor: u8,
    #[cfg(feature = "cluster")]
    remote_peer_cursors: [Option<u64>; 2],
    #[cfg(feature = "cluster")]
    queued_shuffle_bytes: usize,
    #[cfg(feature = "cluster")]
    queued_remote_events: usize,
    #[cfg(feature = "cluster")]
    queued_event_capacity_bytes: usize,
    #[cfg(feature = "cluster")]
    pending_cluster_input: Option<PendingIntervalClusterInput>,
    #[cfg(feature = "cluster")]
    prepared_vnode_transition: Option<PreparedIntervalJoinTransition>,
    #[cfg(feature = "cluster")]
    vnode_transition_cleanup: Option<IntervalJoinTransitionCleanup>,
    applied_left_watermark: i64,
    applied_right_watermark: i64,
    applied_left_idle: bool,
    applied_right_idle: bool,
}

impl IntervalJoinOperator {
    #[cfg(test)]
    pub(crate) fn new(
        name: &str,
        config: StreamJoinConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
    ) -> Self {
        Self::new_with_key_groups(
            name,
            config,
            projection_sql,
            ctx,
            KeyGroupCount::try_from(1_u16).expect("one test key group is valid"),
        )
    }

    pub(crate) fn new_with_key_groups(
        name: &str,
        config: StreamJoinConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
        key_group_count: KeyGroupCount,
    ) -> Self {
        let vnode_count = u32::from(key_group_count);
        let local_assignment =
            VnodeRegistry::single_owner(vnode_count, LOCAL_NODE_ID).versioned_snapshot();
        Self {
            config,
            key_group_count,
            local_assignment,
            vnode_states: std::iter::repeat_with(|| None)
                .take(usize::from(key_group_count.get()))
                .collect(),
            resident_vnodes: Vec::with_capacity(usize::from(key_group_count.get())),
            dirty_vnodes: vec![false; usize::from(key_group_count.get())],
            dirty_vnode_roster: Vec::with_capacity(usize::from(key_group_count.get())),
            full_vnode_capture_required: true,
            max_managed_state_bytes: crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            input_schemas: None,
            projection: ProjectingJoinState::new(name, ctx, projection_sql, "__interval_tmp"),
            #[cfg(feature = "cluster")]
            cluster_shuffle: None,
            #[cfg(feature = "cluster")]
            cluster_peers: Arc::from([]),
            #[cfg(feature = "cluster")]
            local_frontiers: [InputFrontier::default(); 2],
            #[cfg(feature = "cluster")]
            peer_channels: [BTreeMap::new(), BTreeMap::new()],
            #[cfg(feature = "cluster")]
            last_broadcasts: [InputFrontier::default(); 2],
            #[cfg(feature = "cluster")]
            remote_side_cursor: 0,
            #[cfg(feature = "cluster")]
            remote_peer_cursors: [None; 2],
            #[cfg(feature = "cluster")]
            queued_shuffle_bytes: 0,
            #[cfg(feature = "cluster")]
            queued_remote_events: 0,
            #[cfg(feature = "cluster")]
            queued_event_capacity_bytes: 0,
            #[cfg(feature = "cluster")]
            pending_cluster_input: None,
            #[cfg(feature = "cluster")]
            prepared_vnode_transition: None,
            #[cfg(feature = "cluster")]
            vnode_transition_cleanup: None,
            applied_left_watermark: i64::MIN,
            applied_right_watermark: i64::MIN,
            applied_left_idle: false,
            applied_right_idle: false,
        }
    }

    pub(crate) fn set_input_schemas(&mut self, left: SchemaRef, right: SchemaRef) {
        debug_assert!(self.vnode_states.iter().all(Option::is_none));
        self.input_schemas = Some((left, right));
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn attach_cluster_shuffle(&mut self, config: ClusterShuffleConfig) {
        debug_assert!(self.vnode_states.iter().all(Option::is_none));
        debug_assert!(self.resident_vnodes.is_empty());
        debug_assert!(self.dirty_vnode_roster.is_empty());
        self.key_group_count = KeyGroupCount::try_from(config.registry.vnode_count())
            .expect("vnode registry count must fit the checkpoint key-group ABI");
        self.vnode_states
            .resize_with(config.registry.vnode_count() as usize, || None);
        self.dirty_vnodes
            .resize(config.registry.vnode_count() as usize, false);
        let vnode_count = config.registry.vnode_count() as usize;
        self.resident_vnodes.reserve_exact(vnode_count);
        self.dirty_vnode_roster.reserve_exact(vnode_count);
        self.full_vnode_capture_required = true;
        let assignment = config.registry.versioned_snapshot();
        self.local_assignment = assignment.clone();
        let peers = Self::remote_owner_peers(&assignment, config.self_id);
        for &peer in &peers {
            self.peer_channels[0].entry(peer).or_default();
            self.peer_channels[1].entry(peer).or_default();
        }
        self.cluster_peers = peers.into();
        self.cluster_shuffle = Some(config);
    }

    #[cfg(feature = "cluster")]
    fn remote_owner_peers(assignment: &VnodeAssignmentSnapshot, self_id: NodeId) -> Vec<u64> {
        assignment
            .owners()
            .iter()
            .copied()
            .filter(|owner| !owner.is_unassigned() && *owner != self_id)
            .map(|owner| owner.0)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    #[cfg(feature = "cluster")]
    fn active_cluster_scope(
        &self,
    ) -> Result<(ClusterShuffleConfig, VnodeAssignmentSnapshot, Arc<[u64]>), DbError> {
        let config = self.cluster_shuffle.clone().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] has no cluster shuffle scope",
                self.projection.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let sender_digest = config.sender.active_assignment_digest();
        let receiver_digest = config.receiver.active_assignment_digest();
        if assignment.version() != self.local_assignment.version()
            || !std::ptr::eq(assignment.owners(), self.local_assignment.owners())
            || config.sender.local_id() != config.self_id.0
            || config.receiver.local_id() != config.self_id.0
            || config.sender.incarnation() != config.receiver.incarnation()
            || config.sender.assignment_version() != assignment.version()
            || config.receiver.assignment_version() != assignment.version()
            || config.sender.recovery_gen() != config.receiver.recovery_gen()
            || sender_digest.is_none()
            || sender_digest != receiver_digest
        {
            return Err(DbError::ShuffleNotReady(format!(
                "interval join [{}] cluster ownership is outside its attached assignment",
                self.projection.op_name
            )));
        }
        Ok((config, assignment, Arc::clone(&self.cluster_peers)))
    }

    #[cfg(feature = "cluster")]
    fn validate_frontier(
        &self,
        previous: InputFrontier,
        next: InputFrontier,
        side: JoinInputSide,
    ) -> Result<(), DbError> {
        if next.watermark == Some(i64::MIN)
            || (previous.watermark.is_some() && next.watermark.is_none())
        {
            return Err(DbError::Pipeline(format!(
                "interval join [{}] {} frontier became uninitialized",
                self.projection.op_name,
                side.name()
            )));
        }
        if let (Some(previous), Some(next)) = (previous.watermark, next.watermark) {
            if next < previous {
                return Err(DbError::Pipeline(format!(
                    "interval join [{}] {} frontier regressed from {previous} to {next}",
                    self.projection.op_name,
                    side.name()
                )));
            }
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn max_watermark(current: Option<i64>, floor: Option<i64>) -> Option<i64> {
        match (current, floor) {
            (Some(current), Some(floor)) => Some(current.max(floor)),
            (None, floor) => floor,
            (current, None) => current,
        }
    }

    #[cfg(feature = "cluster")]
    fn watermark_option(watermark: i64) -> Option<i64> {
        (watermark != i64::MIN).then_some(watermark)
    }

    #[cfg(feature = "cluster")]
    fn applied_frontiers(&self) -> [InputFrontier; 2] {
        [
            InputFrontier {
                watermark: Self::watermark_option(self.applied_left_watermark),
                idle: self.applied_left_idle,
            },
            InputFrontier {
                watermark: Self::watermark_option(self.applied_right_watermark),
                idle: self.applied_right_idle,
            },
        ]
    }

    fn accounted_state_bytes(&self) -> usize {
        let bytes = self
            .vnode_states
            .capacity()
            .saturating_mul(std::mem::size_of::<Option<Box<IntervalJoinState>>>())
            .saturating_add(
                self.resident_vnodes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.dirty_vnode_roster
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(self.dirty_vnodes.capacity().div_ceil(8))
            .saturating_add(32)
            .saturating_add(std::mem::size_of::<VnodeAssignmentSnapshot>())
            .saturating_add(self.resident_vnodes.iter().fold(0usize, |total, &vnode| {
                let state = self.vnode_states[vnode as usize]
                    .as_ref()
                    .expect("resident interval vnode must contain state");
                total.saturating_add(state.accounted_state_bytes())
            }));
        #[cfg(feature = "cluster")]
        let bytes = bytes.saturating_add(self.cluster_accounted_bytes());
        bytes
    }

    #[cfg(feature = "cluster")]
    fn cluster_accounted_bytes(&self) -> usize {
        let channels = self
            .peer_channels
            .iter()
            .map(BTreeMap::len)
            .sum::<usize>()
            .saturating_mul(
                std::mem::size_of::<(u64, IntervalPeerChannel)>()
                    .saturating_add(PEER_CHANNEL_ENTRY_CHARGE),
            );
        self.cluster_peers
            .len()
            .saturating_mul(std::mem::size_of::<u64>())
            .saturating_add(channels)
            .saturating_add(self.queued_event_capacity_bytes)
            .saturating_add(self.queued_shuffle_bytes)
            .saturating_add(
                self.pending_cluster_input
                    .as_ref()
                    .map_or(0, |pending| pending.accounted_bytes),
            )
    }

    #[cfg(feature = "cluster")]
    fn transition_accounted_bytes(transition: &PreparedIntervalJoinTransition) -> usize {
        let bytes = transition
            .replacements
            .capacity()
            .saturating_mul(std::mem::size_of::<(u32, Option<Box<IntervalJoinState>>)>())
            .saturating_add(
                transition
                    .resident_vnodes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(32)
            .saturating_add(std::mem::size_of::<VnodeAssignmentSnapshot>())
            .saturating_add(
                transition
                    .replacements
                    .iter()
                    .filter_map(|(_, state)| state.as_ref())
                    .fold(0usize, |total, state| {
                        total.saturating_add(state.accounted_state_bytes())
                    }),
            );
        let channels = transition
            .peer_channels
            .iter()
            .map(BTreeMap::len)
            .sum::<usize>()
            .saturating_mul(
                std::mem::size_of::<(u64, IntervalPeerChannel)>()
                    .saturating_add(PEER_CHANNEL_ENTRY_CHARGE),
            )
            .saturating_add(
                transition
                    .peer_channels
                    .iter()
                    .flat_map(BTreeMap::values)
                    .map(|channel| {
                        channel
                            .events
                            .capacity()
                            .saturating_mul(REMOTE_EVENT_CHARGE)
                    })
                    .sum::<usize>(),
            );
        bytes
            .saturating_add(
                transition
                    .cluster_peers
                    .len()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(channels)
    }

    fn add_resident_vnode(&mut self, vnode: u32) {
        if let Err(index) = self.resident_vnodes.binary_search(&vnode) {
            self.resident_vnodes.insert(index, vnode);
        }
    }

    fn remove_resident_vnode(&mut self, vnode: u32) {
        if let Ok(index) = self.resident_vnodes.binary_search(&vnode) {
            self.resident_vnodes.remove(index);
        }
    }

    fn mark_vnode_dirty(&mut self, vnode: u32) {
        let dirty = self
            .dirty_vnodes
            .get_mut(vnode as usize)
            .expect("interval join dirty vnode must have a state slot");
        if !*dirty {
            *dirty = true;
            self.dirty_vnode_roster.push(vnode);
        }
    }

    fn clear_dirty_vnode_roster(&mut self) {
        for vnode in self.dirty_vnode_roster.drain(..) {
            self.dirty_vnodes[vnode as usize] = false;
        }
    }

    #[cfg(feature = "cluster")]
    fn capture_cluster_checkpoint(
        &self,
        max_capture_bytes: usize,
    ) -> Result<Option<CapturedIntervalCluster>, DbError> {
        let Some(config) = self.cluster_shuffle.as_ref() else {
            if self.queued_remote_events != 0
                || self.queued_shuffle_bytes != 0
                || self.pending_cluster_input.is_some()
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] has cluster state without an attached scope",
                    self.projection.op_name
                )));
            }
            return Ok(None);
        };
        let (_, assignment, peers) = self.active_cluster_scope()?;
        if self.pending_cluster_input.is_some()
            || self.last_broadcasts != self.local_frontiers
            || peers.as_ref() != Self::remote_owner_peers(&assignment, config.self_id).as_slice()
            || self.remote_side_cursor > 1
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] cluster channels are not at a checkpoint boundary",
                self.projection.op_name
            )));
        }
        let effective = self.effective_cluster_frontiers(self.local_frontiers, None)?;
        if effective[0].watermark.unwrap_or(i64::MIN) != self.applied_left_watermark
            || effective[1].watermark.unwrap_or(i64::MIN) != self.applied_right_watermark
            || (self.queued_remote_events == 0
                && (effective[0].idle != self.applied_left_idle
                    || effective[1].idle != self.applied_right_idle))
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] applied cluster frontier is inconsistent",
                self.projection.op_name
            )));
        }

        let allocation =
            |bytes: usize| bytes.checked_add(usize::from(bytes != 0) * HEAP_ALLOCATION_CHARGE);
        let mut requested_retained = 0usize;
        for peer_channels in &self.peer_channels {
            requested_retained = requested_retained
                .checked_add(
                    allocation(
                        peer_channels
                            .len()
                            .checked_mul(std::mem::size_of::<CapturedIntervalChannel>())
                            .ok_or_else(|| self.accounting_error())?,
                    )
                    .ok_or_else(|| self.accounting_error())?,
                )
                .ok_or_else(|| self.accounting_error())?;
            for channel in peer_channels.values() {
                requested_retained = requested_retained
                    .checked_add(
                        allocation(
                            channel
                                .events
                                .len()
                                .checked_mul(std::mem::size_of::<CapturedIntervalEvent>())
                                .ok_or_else(|| self.accounting_error())?,
                        )
                        .ok_or_else(|| self.accounting_error())?,
                    )
                    .ok_or_else(|| self.accounting_error())?;
                for event in &channel.events {
                    if let IntervalRemoteEventPayload::Data(batch) = &event.payload {
                        requested_retained = requested_retained
                            .checked_add(
                                batch
                                    .retained
                                    .heap_bytes()
                                    .and_then(|bytes| bytes.checked_add(RETAINED_BATCH_ARC_CHARGE))
                                    .ok_or_else(|| self.accounting_error())?,
                            )
                            .ok_or_else(|| self.accounting_error())?;
                    }
                }
            }
        }
        if requested_retained > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] channel capture requires {requested_retained} bytes; headroom is {max_capture_bytes} bytes",
                self.projection.op_name
            )));
        }

        let mut queued_bytes = 0usize;
        let mut queued_events = 0usize;
        let mut capacity_bytes = 0usize;
        let mut channels = [Vec::new(), Vec::new()];
        for side in [JoinInputSide::Left, JoinInputSide::Right] {
            let port = side.port();
            if self.peer_channels[port].len() != peers.len()
                || !self.peer_channels[port]
                    .keys()
                    .copied()
                    .eq(peers.iter().copied())
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] {} channel roster is incomplete",
                    self.projection.op_name,
                    side.name()
                )));
            }
            channels[port]
                .try_reserve_exact(peers.len())
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] {} checkpoint channel reservation: {error}",
                        self.projection.op_name,
                        side.name()
                    ))
                })?;
            for (&peer, channel) in &self.peer_channels[port] {
                capacity_bytes = capacity_bytes
                    .checked_add(
                        channel
                            .events
                            .capacity()
                            .checked_mul(REMOTE_EVENT_CHARGE)
                            .ok_or_else(|| self.accounting_error())?,
                    )
                    .ok_or_else(|| self.accounting_error())?;
                let mut accepted = channel.applied;
                let mut previous_recovery = None;
                let mut captured = Vec::new();
                captured
                    .try_reserve_exact(channel.events.len())
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] peer {peer} event capture reservation: {error}",
                            self.projection.op_name
                        ))
                    })?;
                for event in &channel.events {
                    if event.assignment_version != assignment.version()
                        || event.recovery_gen > config.receiver.recovery_gen()
                        || previous_recovery.is_some_and(|previous| event.recovery_gen < previous)
                    {
                        return Err(DbError::Checkpoint(format!(
                            "interval join [{}] peer {peer} queue crossed assignment or recovery",
                            self.projection.op_name
                        )));
                    }
                    previous_recovery = Some(event.recovery_gen);
                    queued_bytes = queued_bytes
                        .checked_add(event.payload_bytes())
                        .ok_or_else(|| self.accounting_error())?;
                    queued_events = queued_events
                        .checked_add(1)
                        .ok_or_else(|| self.accounting_error())?;
                    let event = match &event.payload {
                        IntervalRemoteEventPayload::Data(batch) => {
                            let declared = batch.retained.routed_vnodes();
                            let mut seen = vec![false; declared.len()];
                            let coverage_valid = batch.row_vnodes.iter().all(|vnode| {
                                declared.binary_search(vnode).is_ok_and(|index| {
                                    seen[index] = true;
                                    true
                                })
                            }) && seen.iter().all(|seen| *seen);
                            let expected_bytes = batch
                                .retained
                                .heap_bytes()
                                .and_then(|bytes| bytes.checked_add(RETAINED_BATCH_ARC_CHARGE))
                                .and_then(|bytes| {
                                    batch
                                        .row_vnodes
                                        .len()
                                        .checked_mul(std::mem::size_of::<u32>())
                                        .and_then(|vnodes| vnodes.checked_add(ROW_VNODE_ARC_CHARGE))
                                        .and_then(|vnodes| bytes.checked_add(vnodes))
                                });
                            if accepted.idle
                                || declared.is_empty()
                                || declared.windows(2).any(|pair| pair[0] >= pair[1])
                                || declared.iter().any(|vnode| {
                                    assignment
                                        .owners()
                                        .get(*vnode as usize)
                                        .is_none_or(|owner| *owner != config.self_id)
                                })
                                || batch.row_vnodes.len() != batch.retained.batch().num_rows()
                                || !coverage_valid
                                || expected_bytes != Some(batch.charged_bytes)
                                || batch.retained.peer() != Some(peer)
                                || batch.retained.assignment_version() != Some(assignment.version())
                                || batch.retained.recovery_gen() != Some(event.recovery_gen)
                            {
                                return Err(DbError::Checkpoint(format!(
                                    "interval join [{}] peer {peer} has data behind an idle or invalid channel",
                                    self.projection.op_name
                                )));
                            }
                            CapturedIntervalEvent::Data {
                                recovery_gen: event.recovery_gen,
                                retained: Arc::clone(&batch.retained),
                            }
                        }
                        IntervalRemoteEventPayload::Frontier(frontier) => {
                            let floor = if port == 0 {
                                self.applied_left_watermark
                            } else {
                                self.applied_right_watermark
                            };
                            if accepted.idle
                                && !frontier.idle
                                && Self::watermark_option(floor).is_some_and(|floor| {
                                    frontier.watermark.is_none_or(|watermark| watermark < floor)
                                })
                            {
                                return Err(DbError::Checkpoint(format!(
                                    "interval join [{}] peer {peer} {} revival frontier is below its checkpoint floor",
                                    self.projection.op_name,
                                    side.name()
                                )));
                            }
                            self.validate_frontier(accepted, *frontier, side)?;
                            accepted = *frontier;
                            CapturedIntervalEvent::Frontier {
                                recovery_gen: event.recovery_gen,
                                frontier: *frontier,
                            }
                        }
                    };
                    captured.push(event);
                }
                if accepted != channel.accepted {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] peer {peer} accepted frontier is not derivable",
                        self.projection.op_name
                    )));
                }
                channels[port].push(CapturedIntervalChannel {
                    peer,
                    applied: channel.applied,
                    events: captured,
                });
            }
        }
        if queued_bytes != self.queued_shuffle_bytes
            || queued_events != self.queued_remote_events
            || capacity_bytes != self.queued_event_capacity_bytes
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] channel accounting is inconsistent",
                self.projection.op_name
            )));
        }
        let capture = CapturedIntervalCluster {
            assignment_version: assignment.version(),
            owner_map_digest: self.checkpoint_assignment_identity()?.1,
            self_id: config.self_id.0,
            recovery_gen: config.receiver.recovery_gen(),
            local_frontiers: self.local_frontiers,
            remote_side_cursor: self.remote_side_cursor,
            remote_peer_cursors: self.remote_peer_cursors,
            channels,
        };
        let mut retained = 0usize;
        for channels in &capture.channels {
            retained = retained
                .checked_add(
                    allocation(
                        channels
                            .capacity()
                            .checked_mul(std::mem::size_of::<CapturedIntervalChannel>())
                            .ok_or_else(|| self.accounting_error())?,
                    )
                    .ok_or_else(|| self.accounting_error())?,
                )
                .ok_or_else(|| self.accounting_error())?;
            for channel in channels {
                retained = retained
                    .checked_add(
                        allocation(
                            channel
                                .events
                                .capacity()
                                .checked_mul(std::mem::size_of::<CapturedIntervalEvent>())
                                .ok_or_else(|| self.accounting_error())?,
                        )
                        .ok_or_else(|| self.accounting_error())?,
                    )
                    .ok_or_else(|| self.accounting_error())?;
                for event in &channel.events {
                    if let CapturedIntervalEvent::Data {
                        retained: batch, ..
                    } = event
                    {
                        retained = retained
                            .checked_add(
                                batch
                                    .heap_bytes()
                                    .and_then(|bytes| bytes.checked_add(RETAINED_BATCH_ARC_CHARGE))
                                    .ok_or_else(|| self.accounting_error())?,
                            )
                            .ok_or_else(|| self.accounting_error())?;
                    }
                }
            }
        }
        if retained > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] channel capture retains {retained} bytes; headroom is {max_capture_bytes} bytes",
                self.projection.op_name
            )));
        }
        Ok(Some(capture))
    }

    fn capture_operator_checkpoint(
        &self,
        max_capture_bytes: u64,
    ) -> Result<Option<IntervalJoinOperatorCheckpointCapture>, DbError> {
        #[cfg(feature = "cluster")]
        let cluster_attached = self.cluster_shuffle.is_some();
        #[cfg(not(feature = "cluster"))]
        let cluster_attached = false;
        if !cluster_attached
            && self.applied_left_watermark == i64::MIN
            && self.applied_right_watermark == i64::MIN
            && !self.applied_left_idle
            && !self.applied_right_idle
        {
            return Ok(None);
        }
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "interval join [{}] configured time bound exceeds the supported millisecond range",
                self.projection.op_name
            ))
        })?;
        let preflight_bytes = IntervalJoinOperatorCheckpointCapture::calculate_retained_bytes_for(
            self.config.left_keys.len(),
            self.config.right_keys.len(),
            self.config
                .left_keys
                .iter()
                .chain(&self.config.right_keys)
                .chain([
                    &self.config.left_time_column,
                    &self.config.right_time_column,
                    &self.config.left_table,
                    &self.config.right_table,
                ])
                .map(String::len),
        )?;
        if preflight_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] whole checkpoint capture requires at least {preflight_bytes} bytes; capture headroom is {max_capture_bytes} bytes",
                self.projection.op_name
            )));
        }
        #[cfg(feature = "cluster")]
        let cluster = self.capture_cluster_checkpoint(
            usize::try_from(max_capture_bytes.saturating_sub(preflight_bytes))
                .unwrap_or(usize::MAX),
        )?;
        let checkpoint = IntervalJoinOperatorCheckpoint {
            version: OPERATOR_CHECKPOINT_VERSION,
            join_type: join_type_tag(self.config.join_type),
            left_keys: self.config.left_keys.clone(),
            right_keys: self.config.right_keys.clone(),
            left_time_column: self.config.left_time_column.clone(),
            right_time_column: self.config.right_time_column.clone(),
            left_table: self.config.left_table.clone(),
            right_table: self.config.right_table.clone(),
            bound_ms,
            applied_left_watermark: self.applied_left_watermark,
            applied_right_watermark: self.applied_right_watermark,
            applied_left_idle: self.applied_left_idle,
            applied_right_idle: self.applied_right_idle,
            cluster: None,
        };
        let mut capture = IntervalJoinOperatorCheckpointCapture {
            checkpoint,
            #[cfg(feature = "cluster")]
            cluster,
            retained_bytes: 0,
        };
        let retained_bytes = capture.calculate_retained_bytes()?;
        capture.retained_bytes = retained_bytes;
        if retained_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] whole checkpoint capture retains {retained_bytes} bytes; capture headroom is {max_capture_bytes} bytes",
                self.projection.op_name
            )));
        }
        debug_assert_eq!(capture.calculate_retained_bytes()?, retained_bytes);
        Ok(Some(capture))
    }

    fn encode_state_capture(
        capture: IntervalJoinCheckpointCapture,
        context: &str,
        max_encoded_bytes: usize,
    ) -> Result<EncodedStateFrame, DbError> {
        let checkpoint = capture.encode(max_encoded_bytes)?;
        let retained_checkpoint_bytes = checkpoint.retained_ipc_bytes()?;
        let archive_budget = max_encoded_bytes
            .checked_sub(retained_checkpoint_bytes)
            .and_then(|bytes| bytes.checked_sub(HEAP_ALLOCATION_CHARGE))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "{context}: encoded checkpoint retains {retained_checkpoint_bytes} bytes plus its archive allocation; state-frame budget is {max_encoded_bytes} bytes"
                ))
            })?;
        let mut bounded = laminar_core::serialization::BoundedBytesWriter::new(archive_budget);
        let header = vnode_frame_header(PRESENT_VNODE);
        std::io::Write::write_all(&mut bounded, &header).map_err(|error| {
            DbError::Checkpoint(format!("{context}: vnode frame header: {error}"))
        })?;
        let writer = rkyv::ser::writer::IoWriter::new(bounded);
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&checkpoint, writer)
            .map(|bytes| EncodedStateFrame::from_vec(bytes.into_inner().into_vec()))
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "{context}: archive serialization exceeded its {archive_budget}-byte headroom within the {max_encoded_bytes}-byte state-frame budget: {error}"
                ))
            })
    }

    #[cfg(test)]
    fn serialize_state(
        state: &mut IntervalJoinState,
        config: &StreamJoinConfig,
        context: &str,
        max_encoded_bytes: usize,
    ) -> Result<Vec<u8>, DbError> {
        Self::encode_state_capture(
            state.capture_checkpoint(config)?,
            context,
            max_encoded_bytes,
        )
        .map(|bytes| bytes.into_bytes().to_vec())
    }

    fn deserialize_state(
        bytes: &[u8],
        config: &StreamJoinConfig,
        context: &str,
        max_state_bytes: usize,
        cut: Option<IntervalHandoffCut>,
    ) -> Result<IntervalJoinState, DbError> {
        let checkpoint = rkyv::from_bytes::<JoinStateCheckpoint, rkyv::rancor::Error>(bytes)
            .map_err(|error| DbError::Checkpoint(format!("{context}: {error}")))?;
        if let Some(cut) = cut {
            Self::validate_handoff_cutoffs(
                checkpoint.left_evicted_cutoff,
                checkpoint.right_evicted_cutoff,
                checkpoint.left_buffer_rows != 0,
                checkpoint.right_buffer_rows != 0,
                config,
                context,
                cut,
            )?;
        }
        IntervalJoinState::from_checkpoint(&checkpoint, config, max_state_bytes)
            .map_err(|error| DbError::Checkpoint(format!("{context}: {error}")))
    }

    fn decode_vnode_frame(
        bytes: &[u8],
        config: &StreamJoinConfig,
        context: &str,
        max_state_bytes: usize,
        cut: Option<IntervalHandoffCut>,
    ) -> Result<Option<IntervalJoinState>, DbError> {
        if bytes.len() < VNODE_FRAME_HEADER_LEN {
            return Err(DbError::Checkpoint(format!(
                "{context}: vnode frame header is truncated"
            )));
        }
        let (header, payload) = bytes.split_at(VNODE_FRAME_HEADER_LEN);
        if header[1] != VNODE_FRAME_VERSION {
            return Err(DbError::Checkpoint(format!(
                "{context}: vnode frame version {} is unsupported",
                header[1]
            )));
        }
        if header[2..].iter().any(|byte| *byte != 0) {
            return Err(DbError::Checkpoint(format!(
                "{context}: vnode frame header is malformed"
            )));
        }
        let tag = header[0];
        match tag {
            ABSENT_VNODE if payload.is_empty() => Ok(None),
            ABSENT_VNODE => Err(DbError::Checkpoint(format!(
                "{context}: absent vnode frame has a payload"
            ))),
            PRESENT_VNODE if payload.is_empty() => Err(DbError::Checkpoint(format!(
                "{context}: present vnode frame has no payload"
            ))),
            PRESENT_VNODE => {
                Self::deserialize_state(payload, config, context, max_state_bytes, cut).map(Some)
            }
            _ => Err(DbError::Checkpoint(format!(
                "{context}: vnode frame has unknown tag {tag}"
            ))),
        }
    }

    fn validate_handoff_cutoffs(
        left_evicted_cutoff: i64,
        right_evicted_cutoff: i64,
        left_nonempty: bool,
        right_nonempty: bool,
        config: &StreamJoinConfig,
        context: &str,
        cut: IntervalHandoffCut,
    ) -> Result<(), DbError> {
        let bound_ms = i64::try_from(config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "{context}: configured time bound exceeds the supported millisecond range"
            ))
        })?;
        let expected_left_cutoff = cut.right_watermark.saturating_sub(bound_ms);
        let expected_right_cutoff = cut.left_watermark;
        if left_evicted_cutoff > expected_left_cutoff
            || right_evicted_cutoff > expected_right_cutoff
            || (left_nonempty && left_evicted_cutoff != expected_left_cutoff)
            || (right_nonempty && right_evicted_cutoff != expected_right_cutoff)
        {
            return Err(DbError::Checkpoint(format!(
                "{context}: vnode eviction state is inconsistent with the portable handoff cut"
            )));
        }
        Ok(())
    }

    fn validate_checkpoint_config(
        &self,
        checkpoint: &IntervalJoinOperatorCheckpoint,
    ) -> Result<(), DbError> {
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "interval join [{}] configured time bound exceeds the supported millisecond range",
                self.projection.op_name
            ))
        })?;
        if checkpoint.version != OPERATOR_CHECKPOINT_VERSION
            || checkpoint.join_type != join_type_tag(self.config.join_type)
            || checkpoint.left_keys != self.config.left_keys
            || checkpoint.right_keys != self.config.right_keys
            || checkpoint.left_time_column != self.config.left_time_column
            || checkpoint.right_time_column != self.config.right_time_column
            || checkpoint.left_table != self.config.left_table
            || checkpoint.right_table != self.config.right_table
            || checkpoint.bound_ms != bound_ms
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint version or configuration does not match the operator",
                self.projection.op_name
            )));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_assignment_identity(&self) -> Result<(u64, [u8; 32], u64), DbError> {
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] has no cluster assignment",
                self.projection.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let owners = assignment
            .owners()
            .iter()
            .map(|owner| owner.0)
            .collect::<Vec<_>>();
        let owner_map_digest =
            laminar_core::checkpoint::CheckpointAssignmentFence::owner_map_digest(
                u32::from(self.key_group_count),
                &owners,
            );
        let sender_digest = config.sender.active_assignment_digest();
        let receiver_digest = config.receiver.active_assignment_digest();
        if assignment.version() != self.local_assignment.version()
            || assignment.owners() != self.local_assignment.owners()
            || config.sender.local_id() != config.self_id.0
            || config.receiver.local_id() != config.self_id.0
            || config.sender.incarnation() != config.receiver.incarnation()
            || config.sender.assignment_version() != assignment.version()
            || config.receiver.assignment_version() != assignment.version()
            || sender_digest.is_none()
            || sender_digest != receiver_digest
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint assignment is not active",
                self.projection.op_name
            )));
        }
        Ok((assignment.version(), owner_map_digest, config.self_id.0))
    }

    #[cfg(feature = "cluster")]
    fn portable_handoff_cut(
        &self,
        transition: &ManagedVnodeTransition<'_>,
        requires_handoff_cut: bool,
    ) -> Result<Option<IntervalHandoffCut>, DbError> {
        if !requires_handoff_cut {
            return Ok(None);
        }
        let mut donors = std::collections::BTreeSet::new();
        for restore in transition.restores {
            let predecessor_owner = match transition.mode {
                ManagedVnodeTransitionMode::Live => self
                    .local_assignment
                    .owners()
                    .get(restore.vnode as usize)
                    .copied(),
                ManagedVnodeTransitionMode::CheckpointBootstrap { predecessor_owners } => {
                    predecessor_owners.get(restore.vnode as usize).copied()
                }
            };
            if !transition.predecessor.contains(restore.participant_id)
                || predecessor_owner.map(|owner| owner.0) != Some(restore.participant_id)
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] vnode {} restore has invalid donor {}",
                    self.projection.op_name, restore.vnode, restore.participant_id
                )));
            }
            donors.insert(restore.participant_id);
        }
        if donors.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] handoff transition has no acquired vnode frames",
                self.projection.op_name
            )));
        }
        if transition.whole_restores.is_empty() {
            return Ok(None);
        }

        let mut whole_donors = std::collections::BTreeSet::new();
        let mut common: Option<IntervalHandoffCut> = None;
        for restore in transition.whole_restores {
            if !whole_donors.insert(restore.participant_id)
                || restore.state.len() > self.max_managed_state_bytes
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] has an invalid whole frame for donor {}",
                    self.projection.op_name, restore.participant_id
                )));
            }
            let checkpoint =
                rkyv::from_bytes::<IntervalJoinOperatorCheckpoint, rkyv::rancor::Error>(
                    restore.state,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] donor {} whole checkpoint: {error}",
                        self.projection.op_name, restore.participant_id
                    ))
                })?;
            self.validate_checkpoint_config(&checkpoint)?;
            let Some(cluster) = checkpoint.cluster.as_ref() else {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] donor {} whole checkpoint has no cluster identity",
                    self.projection.op_name, restore.participant_id
                )));
            };
            if cluster.assignment_version != transition.predecessor.assignment_version
                || cluster.owner_map_digest != transition.predecessor.assignment_digest
                || cluster.self_id != restore.participant_id
                || cluster
                    .channels
                    .iter()
                    .flatten()
                    .any(|channel| !channel.events.is_empty())
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] donor {} whole checkpoint is not a portable predecessor cut",
                    self.projection.op_name, restore.participant_id
                )));
            }
            let cut = IntervalHandoffCut {
                left_watermark: checkpoint.applied_left_watermark,
                right_watermark: checkpoint.applied_right_watermark,
                left_idle: checkpoint.applied_left_idle,
                right_idle: checkpoint.applied_right_idle,
            };
            if let Some(expected) = &mut common {
                if expected.left_watermark != cut.left_watermark
                    || expected.right_watermark != cut.right_watermark
                    || expected.left_idle != cut.left_idle
                    || expected.right_idle != cut.right_idle
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] donor whole checkpoints disagree on the handoff watermarks",
                        self.projection.op_name
                    )));
                }
            } else {
                common = Some(cut);
            }
        }
        if whole_donors != donors {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] whole checkpoints do not exactly cover acquired vnode donors",
                self.projection.op_name
            )));
        }
        Ok(common)
    }

    #[cfg(feature = "cluster")]
    fn restored_handoff_cut_evidence(
        &self,
        state: &IntervalJoinState,
        context: &str,
    ) -> Result<Option<IntervalHandoffCut>, DbError> {
        let (left_rows, right_rows) = state.buffered_rows();
        let (left_evicted_cutoff, right_evicted_cutoff) = state.evicted_cutoffs();
        if left_rows == 0
            && right_rows == 0
            && left_evicted_cutoff == i64::MIN
            && right_evicted_cutoff == i64::MIN
        {
            return Ok(None);
        }
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "{context}: configured time bound exceeds the supported millisecond range"
            ))
        })?;
        let right_watermark = if left_evicted_cutoff == i64::MIN {
            i64::MIN
        } else {
            left_evicted_cutoff.checked_add(bound_ms).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "{context}: left eviction cutoff cannot be converted to a handoff watermark"
                ))
            })?
        };
        Ok(Some(IntervalHandoffCut {
            left_watermark: right_evicted_cutoff,
            right_watermark,
            left_idle: false,
            right_idle: false,
        }))
    }

    fn execute_shard_cycle(
        &mut self,
        vnode: u32,
        left: &[RecordBatch],
        right: &[RecordBatch],
        left_watermark: i64,
        right_watermark: i64,
        accounted_total: &mut usize,
        output_budget: &mut IntervalJoinOutputBudget,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_admission_watermark = self.applied_left_watermark;
        let right_admission_watermark = self.applied_right_watermark;
        let state_slots = self.vnode_states.len();
        let shard_bytes = self
            .vnode_states
            .get(vnode as usize)
            .and_then(Option::as_ref)
            .map_or(0, |state| state.accounted_state_bytes());
        let other_state_bytes = (*accounted_total).checked_sub(shard_bytes).ok_or_else(|| {
            DbError::BackpressureFail(format!(
                "interval join [{}] retained-state accounting underflow",
                self.projection.op_name
            ))
        })?;
        let shard_limit = self
            .max_managed_state_bytes
            .checked_sub(other_state_bytes)
            .ok_or_else(|| {
                DbError::BackpressureFail(format!(
                    "interval join [{}] already exceeds its {}-byte retained-state limit",
                    self.projection.op_name, self.max_managed_state_bytes
                ))
            })?;
        let has_input = left.iter().any(|batch| batch.num_rows() > 0)
            || right.iter().any(|batch| batch.num_rows() > 0);
        let vnode_index = vnode as usize;
        let slot = self.vnode_states.get(vnode_index).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] routed vnode {vnode} outside its {}-vnode state table",
                self.projection.op_name, state_slots
            ))
        })?;
        if slot.is_none() && !has_input {
            return Ok(Vec::new());
        }
        if slot.is_none() && IntervalJoinState::new().accounted_state_bytes() > shard_limit {
            return Err(DbError::BackpressureFail(format!(
                "interval join [{}] cannot allocate vnode {vnode} state within its {shard_limit}-byte remaining limit",
                self.projection.op_name
            )));
        }
        if slot.is_none() {
            let mut state = IntervalJoinState::new();
            if let Some((left_schema, right_schema)) = &self.input_schemas {
                state.seed_input_schemas(
                    left_schema.clone(),
                    right_schema.clone(),
                    &self.config,
                )?;
            }
            self.vnode_states[vnode_index] = Some(Box::new(state));
            self.add_resident_vnode(vnode);
        }
        let state = self.vnode_states[vnode_index]
            .as_mut()
            .expect("interval join state initialized");
        let cutoffs_before = state.evicted_cutoffs();
        let result = execute_interval_join_cycle(
            state,
            left,
            right,
            &self.config,
            left_admission_watermark,
            right_admission_watermark,
            left_watermark,
            right_watermark,
            shard_limit,
            output_budget,
        );
        *accounted_total = other_state_bytes.saturating_add(state.accounted_state_bytes());
        let checkpoint_state_changed = has_input || state.evicted_cutoffs() != cutoffs_before;
        if result.is_ok() && checkpoint_state_changed {
            self.mark_vnode_dirty(vnode);
        }
        result
    }

    fn execute_routed_shards(
        &mut self,
        routed: BTreeMap<u32, [Vec<RecordBatch>; 2]>,
        left_watermark: i64,
        right_watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let mut accounted_total = self.accounted_state_bytes();
        let mut output_budget = IntervalJoinOutputBudget::default();
        let mut output = Vec::new();
        let mut prior_shard_completed = false;
        for (vnode, [left, right]) in routed {
            match self.execute_shard_cycle(
                vnode,
                &left,
                &right,
                left_watermark,
                right_watermark,
                &mut accounted_total,
                &mut output_budget,
            ) {
                Ok(shard_output) => {
                    output.extend(shard_output);
                    prior_shard_completed = true;
                }
                Err(error) if prior_shard_completed => {
                    let error = if error.requires_pipeline_recovery()
                        || error.requires_pipeline_halt()
                    {
                        error
                    } else {
                        DbError::StatefulOperatorPartialApply(format!(
                            "interval join [{}] admitted an earlier vnode before vnode {vnode} failed: {error}",
                            self.projection.op_name
                        ))
                    };
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(output)
    }

    async fn project_output(
        &mut self,
        join_result: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.projection.apply(join_result).await.map_err(|error| {
            if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
                error
            } else {
                DbError::StatefulOperatorPartialApply(format!(
                    "interval join [{}] admitted input before post-projection failed: {error}",
                    self.projection.op_name
                ))
            }
        })
    }

    fn push_routed_batch(
        routed: &mut BTreeMap<u32, [Vec<RecordBatch>; 2]>,
        vnode: u32,
        side: JoinInputSide,
        batch: RecordBatch,
    ) {
        let port = match side {
            JoinInputSide::Left => 0,
            JoinInputSide::Right => 1,
        };
        routed.entry(vnode).or_default()[port].push(batch);
    }

    fn add_resident_vnodes(&self, routed: &mut BTreeMap<u32, [Vec<RecordBatch>; 2]>) {
        for &vnode in &self.resident_vnodes {
            routed.entry(vnode).or_default();
        }
    }

    fn prevalidate_inputs(&self, inputs: &[Vec<RecordBatch>]) -> Result<(), DbError> {
        if self.config.left_keys.is_empty()
            || self.config.left_keys.len() != self.config.right_keys.len()
            || self.config.left_time_column.is_empty()
            || self.config.right_time_column.is_empty()
            || self.config.time_bound.is_zero()
            || i64::try_from(self.config.time_bound.as_millis()).is_err()
        {
            return Err(DbError::InvalidOperation(
                "interval join requires equal non-empty ordered key vectors, both event-time columns, and a positive finite time bound"
                    .into(),
            ));
        }

        for (port, side, key_names, time_name) in [
            (
                0,
                "left",
                self.config.left_keys.as_slice(),
                self.config.left_time_column.as_str(),
            ),
            (
                1,
                "right",
                self.config.right_keys.as_slice(),
                self.config.right_time_column.as_str(),
            ),
        ] {
            let expected_schema =
                self.input_schemas
                    .as_ref()
                    .map(|schemas| if port == 0 { &schemas.0 } else { &schemas.1 });
            for (batch_index, batch) in inputs.get(port).into_iter().flatten().enumerate() {
                if let Some(expected) = expected_schema {
                    if batch.schema().as_ref() != expected.as_ref() {
                        return Err(DbError::SchemaMismatch(format!(
                            "interval join [{0}] {side} batch {batch_index} does not match its declared schema",
                            self.projection.op_name
                        )));
                    }
                }
                for key_name in key_names {
                    let index = batch.schema().index_of(key_name).map_err(|error| {
                        DbError::SchemaMismatch(format!(
                            "interval join [{}] {side} key '{key_name}': {error}",
                            self.projection.op_name
                        ))
                    })?;
                    if !matches!(
                        batch.column(index).data_type(),
                        DataType::Utf8 | DataType::Int64
                    ) {
                        return Err(DbError::SchemaMismatch(format!(
                            "interval join [{}] {side} key '{key_name}' must be Utf8 or Int64, found {}",
                            self.projection.op_name,
                            batch.column(index).data_type()
                        )));
                    }
                }
                let time_index = batch.schema().index_of(time_name).map_err(|error| {
                    DbError::SchemaMismatch(format!(
                        "interval join [{}] {side} event-time column '{time_name}': {error}",
                        self.projection.op_name
                    ))
                })?;
                let time_column = batch.column(time_index);
                if !matches!(time_column.data_type(), DataType::Timestamp(_, _))
                    || time_column.null_count() != 0
                {
                    return Err(DbError::SchemaMismatch(format!(
                        "interval join [{}] {side} event-time column '{time_name}' must be a non-null Timestamp, found {} with {} nulls",
                        self.projection.op_name,
                        time_column.data_type(),
                        time_column.null_count()
                    )));
                }
            }
        }
        Ok(())
    }

    fn route_local_inputs(
        &self,
        inputs: &[Vec<RecordBatch>],
    ) -> Result<BTreeMap<u32, [Vec<RecordBatch>; 2]>, DbError> {
        self.prevalidate_inputs(inputs)?;
        let mut routed = BTreeMap::new();
        let vnode_count = u32::from(self.key_group_count);

        for (side, batches) in [
            (
                JoinInputSide::Left,
                inputs.first().map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
            (
                JoinInputSide::Right,
                inputs.get(1).map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
        ] {
            let (side_name, key_names) = match side {
                JoinInputSide::Left => ("left", self.config.left_keys.as_slice()),
                JoinInputSide::Right => ("right", self.config.right_keys.as_slice()),
            };
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let key_indices = key_names
                    .iter()
                    .map(|key_name| {
                        batch.schema().index_of(key_name).map_err(|error| {
                            DbError::SchemaMismatch(format!(
                                "interval join [{}] {side_name} routing key '{key_name}': {error}",
                                self.projection.op_name
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let row_vnodes =
                    laminar_core::shuffle::row_vnodes(batch, &key_indices, vnode_count).map_err(
                        |error| {
                            DbError::Pipeline(format!(
                                "interval join [{}] {side_name} routing: {error}",
                                self.projection.op_name
                            ))
                        },
                    )?;
                let plan = laminar_core::shuffle::route_checkpointed_batch(
                    batch,
                    &row_vnodes,
                    &self.local_assignment,
                    LOCAL_NODE_ID,
                )
                .map_err(|error| {
                    DbError::Pipeline(format!(
                        "interval join [{}] {side_name} routing: {error}",
                        self.projection.op_name
                    ))
                })?;
                if !plan.remote.is_empty() {
                    return Err(DbError::Pipeline(format!(
                        "interval join [{}] local topology routed rows off-node",
                        self.projection.op_name
                    )));
                }
                for route in plan.local {
                    Self::push_routed_batch(&mut routed, route.vnode, side, route.batch);
                }
            }
        }
        Ok(routed)
    }

    #[cfg(feature = "cluster")]
    fn row_vnodes_for_side(
        &self,
        side: JoinInputSide,
        batch: &RecordBatch,
        vnode_count: u32,
    ) -> Result<Vec<u32>, DbError> {
        let (side_name, key_names) = match side {
            JoinInputSide::Left => ("left", self.config.left_keys.as_slice()),
            JoinInputSide::Right => ("right", self.config.right_keys.as_slice()),
        };
        let key_indices = key_names
            .iter()
            .map(|key_name| {
                batch.schema().index_of(key_name).map_err(|error| {
                    DbError::SchemaMismatch(format!(
                        "interval join [{}] {side_name} routing key '{key_name}': {error}",
                        self.projection.op_name
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        laminar_core::shuffle::row_vnodes(batch, &key_indices, vnode_count).map_err(|error| {
            crate::operator::shuffle_routing_error(
                &format!(
                    "interval join [{}] {side_name} routing",
                    self.projection.op_name
                ),
                &error,
            )
        })
    }

    #[cfg(feature = "cluster")]
    fn build_queued_batch(
        &self,
        retained: crate::operator::RetainedBatch,
        accepted: InputFrontier,
        config: &ClusterShuffleConfig,
        assignment: &laminar_core::state::VnodeAssignmentSnapshot,
        side: JoinInputSide,
    ) -> Result<QueuedIntervalBatch, DbError> {
        if accepted.idle {
            return Err(DbError::ShuffleTerminal(format!(
                "interval join [{}] received {} data behind an idle peer frontier",
                self.projection.op_name,
                side.name()
            )));
        }
        let batch = retained.batch();
        let declared = retained.routed_vnodes();
        if batch.num_rows() == 0
            || batch.num_rows() > laminar_core::shuffle::ROUTE_MAX_BATCH_ROWS
            || declared.is_empty()
            || declared.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(DbError::ShuffleTerminal(format!(
                "interval join [{}] rejected non-canonical {} shuffle data",
                self.projection.op_name,
                side.name()
            )));
        }
        let logical_bytes = laminar_core::shuffle::logical_batch_bytes(batch).map_err(|error| {
            DbError::ShuffleTerminal(format!(
                "interval join [{}] rejected {} shuffle batch size: {error}",
                self.projection.op_name,
                side.name()
            ))
        })?;
        if logical_bytes > laminar_core::shuffle::ROUTE_MAX_BATCH_BYTES
            || declared.iter().any(|vnode| {
                assignment
                    .owners()
                    .get(*vnode as usize)
                    .is_none_or(|owner| *owner != config.self_id)
            })
        {
            return Err(DbError::ShuffleTerminal(format!(
                "interval join [{}] received {} data outside local vnode ownership",
                self.projection.op_name,
                side.name()
            )));
        }
        let row_vnodes = self.row_vnodes_for_side(side, batch, config.registry.vnode_count())?;
        let mut seen = vec![false; declared.len()];
        for vnode in &row_vnodes {
            let Ok(index) = declared.binary_search(vnode) else {
                return Err(DbError::ShuffleTerminal(format!(
                    "interval join [{}] {} shuffle vnode metadata omits a decoded row",
                    self.projection.op_name,
                    side.name()
                )));
            };
            seen[index] = true;
        }
        if seen.iter().any(|seen| !seen) {
            return Err(DbError::ShuffleTerminal(format!(
                "interval join [{}] {} shuffle vnode metadata names an absent row",
                self.projection.op_name,
                side.name()
            )));
        }
        let charged_bytes = retained
            .heap_bytes()
            .and_then(|bytes| bytes.checked_add(RETAINED_BATCH_ARC_CHARGE))
            .and_then(|bytes| {
                row_vnodes
                    .len()
                    .checked_mul(std::mem::size_of::<u32>())
                    .and_then(|vnodes| vnodes.checked_add(ROW_VNODE_ARC_CHARGE))
                    .and_then(|vnodes| bytes.checked_add(vnodes))
            })
            .ok_or_else(|| self.accounting_error())?;
        Ok(QueuedIntervalBatch {
            retained: Arc::new(retained),
            row_vnodes: row_vnodes.into(),
            charged_bytes,
        })
    }

    #[cfg(feature = "cluster")]
    fn route_owned_batch(
        &self,
        config: &ClusterShuffleConfig,
        assignment: &laminar_core::state::VnodeAssignmentSnapshot,
        side: JoinInputSide,
        batch: &RecordBatch,
        row_vnodes: &[u32],
        routed: &mut BTreeMap<u32, [Vec<RecordBatch>; 2]>,
    ) -> Result<(), DbError> {
        let side_name = side.name();
        let plan = laminar_core::shuffle::route_checkpointed_batch(
            batch,
            row_vnodes,
            assignment,
            config.self_id,
        )
        .map_err(|error| {
            crate::operator::shuffle_routing_error(
                &format!(
                    "interval join [{}] {side_name} ownership validation",
                    self.projection.op_name
                ),
                &error,
            )
        })?;
        if !plan.remote.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] received {side_name} shuffle data no longer owned by this node",
                self.projection.op_name
            )));
        }
        for route in plan.local {
            Self::push_routed_batch(routed, route.vnode, side, route.batch);
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn effective_cluster_frontiers(
        &self,
        local: [InputFrontier; 2],
        override_event: Option<(usize, u64, InputFrontier, usize)>,
    ) -> Result<[InputFrontier; 2], DbError> {
        let merge = |port: usize| -> Result<InputFrontier, DbError> {
            let peers = self.peer_channels[port].iter().map(|(&peer, channel)| {
                let mut applied = override_event
                    .filter(|(event_port, event_peer, _, _)| {
                        *event_port == port && *event_peer == peer
                    })
                    .map_or(channel.applied, |(_, _, frontier, _)| frontier);
                let consumed = override_event
                    .filter(|(event_port, event_peer, _, _)| {
                        *event_port == port && *event_peer == peer
                    })
                    .map_or(0, |(_, _, _, consumed)| consumed);
                if channel.events.len() > consumed {
                    applied.idle = false;
                    let floor = if port == 0 {
                        self.applied_left_watermark
                    } else {
                        self.applied_right_watermark
                    };
                    applied.watermark =
                        Self::max_watermark(applied.watermark, Self::watermark_option(floor));
                }
                applied
            });
            let merged =
                merge_input_frontier_iter(std::iter::once(local[port]).chain(peers), i64::MIN);
            let previous = self.applied_frontiers()[port];
            self.validate_frontier(
                previous,
                merged,
                if port == 0 {
                    JoinInputSide::Left
                } else {
                    JoinInputSide::Right
                },
            )?;
            if self.pending_cluster_input.is_some() {
                return Ok(InputFrontier {
                    watermark: previous.watermark,
                    idle: false,
                });
            }
            Ok(merged)
        };
        Ok([merge(0)?, merge(1)?])
    }

    #[cfg(feature = "cluster")]
    fn plan_cluster_inputs(
        &self,
        inputs: &[Vec<RecordBatch>],
        frontiers: [InputFrontier; 2],
        config: &ClusterShuffleConfig,
        assignment: &VnodeAssignmentSnapshot,
        peers: &[u64],
    ) -> Result<IntervalClusterInputPlan, DbError> {
        self.prevalidate_inputs(inputs)?;
        let mut local_frontiers = frontiers;
        for side in [JoinInputSide::Left, JoinInputSide::Right] {
            let port = side.port();
            self.validate_frontier(self.local_frontiers[port], frontiers[port], side)?;
            let has_data = inputs
                .get(port)
                .is_some_and(|batches| batches.iter().any(|batch| batch.num_rows() != 0));
            if frontiers[port].idle && has_data {
                return Err(DbError::InvalidOperation(format!(
                    "interval join [{}] received {} data from an idle local channel",
                    self.projection.op_name,
                    side.name()
                )));
            }
            if self.local_frontiers[port].idle && !local_frontiers[port].idle {
                let floor = if port == 0 {
                    self.applied_left_watermark
                } else {
                    self.applied_right_watermark
                };
                local_frontiers[port].watermark = Self::max_watermark(
                    local_frontiers[port].watermark,
                    Self::watermark_option(floor),
                );
            }
        }

        let mut routed = BTreeMap::new();
        let mut remote_data: [BTreeMap<u64, Vec<laminar_core::shuffle::ShuffleMessage>>; 2] =
            [BTreeMap::new(), BTreeMap::new()];
        for side in [JoinInputSide::Right, JoinInputSide::Left] {
            let port = side.port();
            let key_names = if port == 0 {
                self.config.left_keys.as_slice()
            } else {
                self.config.right_keys.as_slice()
            };
            let stage = format!("{}::{}", self.projection.op_name, side.name());
            for batch in inputs.get(port).into_iter().flatten() {
                if batch.num_rows() == 0 {
                    continue;
                }
                let key_indices = key_names
                    .iter()
                    .map(|key| batch.schema().index_of(key))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        DbError::SchemaMismatch(format!(
                            "interval join [{}] {} routing key: {error}",
                            self.projection.op_name,
                            side.name()
                        ))
                    })?;
                let row_vnodes = laminar_core::shuffle::row_vnodes(
                    batch,
                    &key_indices,
                    config.registry.vnode_count(),
                )
                .map_err(|error| {
                    crate::operator::shuffle_routing_error(
                        &format!(
                            "interval join [{}] {} routing",
                            self.projection.op_name,
                            side.name()
                        ),
                        &error,
                    )
                })?;
                let plan = laminar_core::shuffle::route_checkpointed_batch(
                    batch,
                    &row_vnodes,
                    assignment,
                    config.self_id,
                )
                .map_err(|error| {
                    crate::operator::shuffle_routing_error(
                        &format!(
                            "interval join [{}] {} routing",
                            self.projection.op_name,
                            side.name()
                        ),
                        &error,
                    )
                })?;
                for route in plan.local {
                    Self::push_routed_batch(&mut routed, route.vnode, side, route.batch);
                }
                for route in plan.remote {
                    remote_data[port].entry(route.owner.0).or_default().push(
                        laminar_core::shuffle::ShuffleMessage::checkpointed_routed(
                            stage.clone(),
                            route.routed_vnodes,
                            route.batch,
                        ),
                    );
                }
            }
        }

        let mut outbound = Vec::new();
        for &peer in peers {
            for side in [JoinInputSide::Right, JoinInputSide::Left] {
                let port = side.port();
                let current = local_frontiers[port];
                let data = remote_data[port].remove(&peer);
                let has_data = data.as_ref().is_some_and(|data| !data.is_empty());
                let stage = format!("{}::{}", self.projection.op_name, side.name());
                if has_data && self.last_broadcasts[port].idle && !current.idle {
                    outbound.push((
                        peer,
                        laminar_core::shuffle::ShuffleMessage::Frontier {
                            stage: stage.clone(),
                            watermark: self.last_broadcasts[port].watermark,
                            idle: false,
                        },
                    ));
                }
                if let Some(data) = data {
                    outbound.extend(data.into_iter().map(|message| (peer, message)));
                }
                if has_data || self.last_broadcasts[port] != current {
                    outbound.push((
                        peer,
                        laminar_core::shuffle::ShuffleMessage::Frontier {
                            stage,
                            watermark: current.watermark,
                            idle: current.idle,
                        },
                    ));
                }
            }
        }
        if remote_data.iter().any(|by_peer| !by_peer.is_empty()) {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] routed data outside its owner frontier roster",
                self.projection.op_name
            )));
        }
        let effective_frontiers = self.effective_cluster_frontiers(local_frontiers, None)?;
        Ok(IntervalClusterInputPlan {
            routed,
            outbound,
            local_frontiers,
            effective_frontiers,
        })
    }

    #[cfg(feature = "cluster")]
    fn batch_plan_bytes(&self, batch: &RecordBatch) -> Result<usize, DbError> {
        batch
            .num_columns()
            .checked_mul(std::mem::size_of::<Arc<dyn arrow::array::Array>>())
            .and_then(|bytes| bytes.checked_add(batch.get_array_memory_size()))
            .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
            .ok_or_else(|| self.accounting_error())
    }

    #[cfg(feature = "cluster")]
    fn cluster_input_plan_bytes(&self, plan: &IntervalClusterInputPlan) -> Result<usize, DbError> {
        let mut bytes = plan
            .routed
            .len()
            .checked_mul(
                std::mem::size_of::<(u32, [Vec<RecordBatch>; 2])>() + PENDING_ROUTE_ENTRY_CHARGE,
            )
            .and_then(|bytes| {
                bytes.checked_add(plan.outbound.capacity().checked_mul(std::mem::size_of::<(
                    u64,
                    laminar_core::shuffle::ShuffleMessage,
                )>())?)
            })
            .and_then(|bytes| {
                plan.outbound
                    .len()
                    .checked_mul(
                        std::mem::size_of::<usize>()
                            + std::mem::size_of::<(u64, usize)>()
                            + std::mem::size_of::<(u64, Vec<usize>)>(),
                    )
                    .and_then(|grouping| bytes.checked_add(grouping))
            })
            .ok_or_else(|| self.accounting_error())?;
        for batches in plan.routed.values().flat_map(|sides| sides.iter()) {
            bytes = bytes
                .checked_add(
                    batches
                        .capacity()
                        .checked_mul(std::mem::size_of::<RecordBatch>())
                        .ok_or_else(|| self.accounting_error())?,
                )
                .ok_or_else(|| self.accounting_error())?;
            for batch in batches {
                bytes = bytes
                    .checked_add(self.batch_plan_bytes(batch)?)
                    .ok_or_else(|| self.accounting_error())?;
            }
        }
        for (_, message) in &plan.outbound {
            let message_bytes = match message {
                laminar_core::shuffle::ShuffleMessage::Barrier(_) => 0,
                laminar_core::shuffle::ShuffleMessage::Frontier { stage, .. } => stage.capacity(),
                laminar_core::shuffle::ShuffleMessage::Data {
                    stage,
                    routed_vnodes,
                    batch,
                } => self
                    .batch_plan_bytes(batch)?
                    .checked_add(stage.capacity())
                    .and_then(|bytes| {
                        bytes.checked_add(
                            routed_vnodes
                                .len()
                                .checked_mul(std::mem::size_of::<u32>())?,
                        )
                    })
                    .ok_or_else(|| self.accounting_error())?,
            };
            bytes = bytes
                .checked_add(message_bytes)
                .ok_or_else(|| self.accounting_error())?;
        }
        Ok(bytes)
    }

    #[cfg(feature = "cluster")]
    fn accounting_error(&self) -> DbError {
        DbError::Pipeline(format!(
            "interval join [{}] managed-state accounting overflow",
            self.projection.op_name
        ))
    }

    #[cfg(feature = "cluster")]
    fn outbound_finalize_error(&self, error: DbError) -> DbError {
        if error.requires_pipeline_recovery() {
            error
        } else {
            DbError::ShufflePartialSend(format!(
                "interval join [{}] failed after outbound shuffle admission: {error}",
                self.projection.op_name
            ))
        }
    }

    #[cfg(feature = "cluster")]
    fn remote_replay_error(&self, error: DbError) -> DbError {
        if error.requires_pipeline_recovery() {
            error
        } else {
            DbError::Checkpoint(format!(
                "interval join [{}] ordered shuffle replay requires recovery: {error}",
                self.projection.op_name
            ))
        }
    }

    #[cfg(feature = "cluster")]
    fn start_pending_cluster_send(
        &mut self,
        config: &ClusterShuffleConfig,
        assignment_version: u64,
    ) {
        let pending = self
            .pending_cluster_input
            .as_mut()
            .expect("interval send plan must be installed before it starts");
        debug_assert!(pending.send.is_none());
        debug_assert!(pending.outcome.is_none());
        let outbound = pending
            .outbound
            .take()
            .expect("idle interval send plan must retain its outbound cut");
        let sender = Arc::clone(&config.sender);
        let wake = config.receiver.work_ready_notify();
        let context = format!("interval join [{}] shuffle", self.projection.op_name);
        let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
        pending.outcome = Some(outcome_rx);
        pending.send = Some(tokio::spawn(async move {
            let outcome = crate::operator::send_shuffle_plan_retaining(
                &sender,
                assignment_version,
                outbound,
                &context,
            )
            .await;
            let should_wake = !matches!(&outcome.0, Err(error) if error.is_shuffle_not_ready());
            if outcome_tx.send(outcome).is_ok() && should_wake {
                wake.notify_one();
            }
        }));
    }

    #[cfg(feature = "cluster")]
    async fn finish_pending_cluster_input(&mut self) -> Result<PendingIntervalCompletion, DbError> {
        let received = {
            let Some(pending) = self.pending_cluster_input.as_mut() else {
                return Ok(PendingIntervalCompletion::Waiting);
            };
            match (pending.send.as_ref(), pending.outcome.as_mut()) {
                (None, None) => return Ok(PendingIntervalCompletion::Waiting),
                (Some(_), Some(outcome)) => outcome.try_recv().map_err(|error| match error {
                    tokio::sync::oneshot::error::TryRecvError::Empty => None,
                    tokio::sync::oneshot::error::TryRecvError::Closed => {
                        Some("send task ended without a delivery outcome")
                    }
                }),
                _ => Err(Some("send task lost its completion channel")),
            }
        };
        let outcome = match received {
            Ok(outcome) => outcome,
            Err(None) => {
                return Ok(PendingIntervalCompletion::Waiting);
            }
            Err(Some(reason)) => {
                drop(self.pending_cluster_input.take());
                return Err(DbError::ShufflePartialSend(format!(
                    "interval join [{}] {reason}",
                    self.projection.op_name
                )));
            }
        };
        let mut pending = self
            .pending_cluster_input
            .take()
            .expect("finished interval send plan");
        pending.send.take().expect("completed interval send task");
        pending
            .outcome
            .take()
            .expect("completed interval send outcome");
        let (result, outbound) = outcome;
        if let Err(error) = result {
            if error.is_shuffle_not_ready() {
                pending.outbound = Some(outbound.ok_or_else(|| {
                    DbError::ShufflePartialSend(format!(
                        "interval join [{}] safe send failure lost its retry plan",
                        self.projection.op_name
                    ))
                })?);
                self.pending_cluster_input = Some(pending);
                return Ok(PendingIntervalCompletion::RetryLater);
            }
            return Err(error);
        }
        debug_assert!(outbound.is_none());
        let effective = self
            .effective_cluster_frontiers(pending.local_frontiers, None)
            .map_err(|error| self.outbound_finalize_error(error))?;
        let routed = std::mem::take(&mut pending.routed);
        let local_frontiers = pending.local_frontiers;
        drop(pending);
        let output = self
            .apply_routed_cluster(routed, effective)
            .map_err(|error| self.outbound_finalize_error(error))?;
        let output = self
            .project_output(output)
            .await
            .map_err(|error| self.outbound_finalize_error(error))?;
        self.local_frontiers = local_frontiers;
        self.last_broadcasts = local_frontiers;
        Ok(PendingIntervalCompletion::Applied(output))
    }

    #[cfg(feature = "cluster")]
    fn apply_routed_cluster(
        &mut self,
        mut routed: BTreeMap<u32, [Vec<RecordBatch>; 2]>,
        frontiers: [InputFrontier; 2],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left = frontiers[0].watermark.unwrap_or(i64::MIN);
        let right = frontiers[1].watermark.unwrap_or(i64::MIN);
        if left > self.applied_left_watermark || right > self.applied_right_watermark {
            self.add_resident_vnodes(&mut routed);
        }
        let output = self.execute_routed_shards(routed, left, right)?;
        self.applied_left_watermark = self.applied_left_watermark.max(left);
        self.applied_right_watermark = self.applied_right_watermark.max(right);
        self.applied_left_idle = frontiers[0].idle;
        self.applied_right_idle = frontiers[1].idle;
        Ok(output)
    }

    #[cfg(feature = "cluster")]
    async fn drain_remote_event(
        &mut self,
        config: &ClusterShuffleConfig,
        assignment: &VnodeAssignmentSnapshot,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let mut selected = None;
        for side_offset in 0..2 {
            let port = (usize::from(self.remote_side_cursor) + side_offset) % 2;
            let side = if port == 0 {
                JoinInputSide::Left
            } else {
                JoinInputSide::Right
            };
            let peers = self.cluster_peers.as_ref();
            if peers.is_empty() {
                continue;
            }
            let start = self.remote_peer_cursors[port].map_or(0, |cursor| {
                let next = peers.partition_point(|peer| *peer <= cursor);
                if next == peers.len() {
                    0
                } else {
                    next
                }
            });
            for offset in 0..peers.len() {
                let peer = peers[(start + offset) % peers.len()];
                if !self.peer_channels[port][&peer].events.is_empty() {
                    selected = Some((side, peer));
                    break;
                }
            }
            if selected.is_some() {
                break;
            }
        }
        let (side, peer) = selected.ok_or_else(|| {
            DbError::Checkpoint("interval join remote-event count is inconsistent".into())
        })?;
        let port = side.port();
        let channel = &self.peer_channels[port][&peer];
        let event = channel.events.front().expect("selected interval event");
        if event.assignment_version != assignment.version()
            || event.recovery_gen > config.receiver.recovery_gen()
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] queued {} event crossed assignment or recovery",
                self.projection.op_name,
                side.name()
            )));
        }
        let mut routed = BTreeMap::new();
        let next_applied = match &event.payload {
            IntervalRemoteEventPayload::Data(batch) => {
                self.route_owned_batch(
                    config,
                    assignment,
                    side,
                    batch.retained.batch(),
                    &batch.row_vnodes,
                    &mut routed,
                )
                .map_err(|error| self.remote_replay_error(error))?;
                channel.applied
            }
            IntervalRemoteEventPayload::Frontier(frontier) => {
                self.validate_frontier(channel.applied, *frontier, side)
                    .map_err(|error| self.remote_replay_error(error))?;
                *frontier
            }
        };
        let effective = self
            .effective_cluster_frontiers(self.local_frontiers, Some((port, peer, next_applied, 1)))
            .map_err(|error| self.remote_replay_error(error))?;
        let released = event.payload_bytes();
        let was_frontier = matches!(event.payload, IntervalRemoteEventPayload::Frontier(_));
        let channel = self.peer_channels[port]
            .get_mut(&peer)
            .expect("selected interval channel");
        channel.events.pop_front().expect("selected interval event");
        if was_frontier {
            channel.applied = next_applied;
        }
        self.remote_peer_cursors[port] = Some(peer);
        self.remote_side_cursor = u8::try_from((port + 1) % 2).expect("interval side cursor");
        self.queued_shuffle_bytes = self
            .queued_shuffle_bytes
            .checked_sub(released)
            .expect("validated interval queue accounting");
        self.queued_remote_events = self
            .queued_remote_events
            .checked_sub(1)
            .expect("validated interval event accounting");
        let output = self
            .apply_routed_cluster(routed, effective)
            .map_err(|error| self.remote_replay_error(error))?;
        self.project_output(output)
            .await
            .map_err(|error| self.remote_replay_error(error))
    }

    #[cfg(feature = "cluster")]
    async fn process_cluster(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        left_frontier: InputFrontier,
        right_frontier: InputFrontier,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let scope = self.active_cluster_scope();
        let (config, assignment, peers) = match scope {
            Ok(scope) => scope,
            Err(error) if self.pending_cluster_input.is_some() => {
                return Err(self.outbound_finalize_error(error));
            }
            Err(error) => return Err(error),
        };
        let mut deferred_output = Vec::new();
        let mut drained_remote = false;
        if self.queued_remote_events != 0 {
            if inputs.iter().any(|batches| !batches.is_empty()) {
                let error = DbError::InvalidOperation(format!(
                    "interval join [{}] received local input while ordered shuffle replay was pending",
                    self.projection.op_name
                ));
                return Err(if self.pending_cluster_input.is_some() {
                    self.outbound_finalize_error(error)
                } else {
                    error
                });
            }
            deferred_output = self.drain_remote_event(&config, &assignment).await?;
            drained_remote = true;
        }
        let completion = self.finish_pending_cluster_input().await.map_err(|error| {
            if drained_remote {
                self.remote_replay_error(error)
            } else {
                error
            }
        })?;
        match completion {
            PendingIntervalCompletion::Applied(output) => {
                deferred_output.extend(output);
                return Ok(deferred_output);
            }
            PendingIntervalCompletion::Waiting | PendingIntervalCompletion::RetryLater => {}
        }
        if self.pending_cluster_input.is_some() {
            if inputs.iter().any(|batches| !batches.is_empty()) {
                return Err(
                    self.outbound_finalize_error(DbError::InvalidOperation(format!(
                        "interval join [{}] received local input while a shuffle send was pending",
                        self.projection.op_name
                    ))),
                );
            }
            if self
                .pending_cluster_input
                .as_ref()
                .is_some_and(|pending| pending.send.is_none())
            {
                self.start_pending_cluster_send(&config, assignment.version());
            }
            return Ok(deferred_output);
        }
        if drained_remote {
            return Ok(deferred_output);
        }
        let plan = self.plan_cluster_inputs(
            inputs,
            [left_frontier, right_frontier],
            &config,
            &assignment,
            &peers,
        )?;
        if !plan.outbound.is_empty() {
            let accounted_bytes = self.cluster_input_plan_bytes(&plan)?;
            let total = self
                .accounted_state_bytes()
                .checked_add(accounted_bytes)
                .ok_or_else(|| self.accounting_error())?;
            if total > self.max_managed_state_bytes {
                return Err(DbError::ManagedStateBudgetExceeded {
                    context: format!(
                        "interval join [{}] pending shuffle send",
                        self.projection.op_name
                    ),
                    accounted_bytes: total,
                    limit_bytes: self.max_managed_state_bytes,
                });
            }
            let IntervalClusterInputPlan {
                routed,
                outbound,
                local_frontiers,
                effective_frontiers: _,
            } = plan;
            self.pending_cluster_input = Some(PendingIntervalClusterInput {
                routed,
                outbound: Some(outbound),
                local_frontiers,
                send: None,
                outcome: None,
                accounted_bytes,
            });
            self.start_pending_cluster_send(&config, assignment.version());
            return Ok(Vec::new());
        }
        let output = self.apply_routed_cluster(plan.routed, plan.effective_frontiers)?;
        let output = self.project_output(output).await?;
        self.local_frontiers = plan.local_frontiers;
        self.last_broadcasts = plan.local_frontiers;
        Ok(output)
    }

    #[cfg(feature = "cluster")]
    fn side_for_stage(&self, stage: &str) -> Result<JoinInputSide, DbError> {
        match stage.strip_prefix(self.projection.op_name.as_ref()) {
            Some("::left") => Ok(JoinInputSide::Left),
            Some("::right") => Ok(JoinInputSide::Right),
            _ => Err(DbError::ShuffleTerminal(format!(
                "interval join [{}] rejected unknown shuffle stage '{stage}'",
                self.projection.op_name
            ))),
        }
    }

    #[cfg(feature = "cluster")]
    fn reserve_remote_event_slot(
        &mut self,
        side: JoinInputSide,
        peer: u64,
        payload_bytes: usize,
    ) -> Result<(usize, usize), DbError> {
        let current_accounted = self.accounted_state_bytes();
        let next_bytes = self
            .queued_shuffle_bytes
            .checked_add(payload_bytes)
            .ok_or_else(|| self.accounting_error())?;
        let next_events = self
            .queued_remote_events
            .checked_add(1)
            .ok_or_else(|| self.accounting_error())?;
        let port = side.port();
        let previous_capacity = self.peer_channels[port][&peer].events.capacity();
        self.peer_channels[port]
            .get_mut(&peer)
            .expect("validated interval peer channel")
            .events
            .try_reserve_exact(1)
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "interval join [{}] could not reserve ordered shuffle event: {error}",
                    self.projection.op_name
                ))
            })?;
        let reserved_capacity = self.peer_channels[port][&peer].events.capacity();
        let added_capacity_bytes = reserved_capacity
            .checked_sub(previous_capacity)
            .and_then(|slots| slots.checked_mul(REMOTE_EVENT_CHARGE))
            .ok_or_else(|| self.accounting_error())?;
        let next_accounted = current_accounted
            .checked_add(added_capacity_bytes)
            .and_then(|bytes| bytes.checked_add(payload_bytes))
            .ok_or_else(|| self.accounting_error())?;
        if next_accounted > self.max_managed_state_bytes {
            self.peer_channels[port]
                .get_mut(&peer)
                .expect("reserved interval peer channel")
                .events
                .shrink_to(previous_capacity);
            let retained_capacity = self.peer_channels[port][&peer].events.capacity();
            self.queued_event_capacity_bytes = self
                .queued_event_capacity_bytes
                .checked_add(
                    retained_capacity
                        .checked_sub(previous_capacity)
                        .and_then(|slots| slots.checked_mul(REMOTE_EVENT_CHARGE))
                        .ok_or_else(|| self.accounting_error())?,
                )
                .ok_or_else(|| self.accounting_error())?;
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "interval join [{}] ordered shuffle queue",
                    self.projection.op_name
                ),
                accounted_bytes: next_accounted,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        self.queued_event_capacity_bytes = self
            .queued_event_capacity_bytes
            .checked_add(added_capacity_bytes)
            .ok_or_else(|| self.accounting_error())?;
        Ok((next_bytes, next_events))
    }
}

#[async_trait]
impl GraphOperator for IntervalJoinOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::bounded_interval_join()
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_aligned_replay_pending(&self) -> bool {
        self.pending_cluster_input.is_some() || self.queued_remote_events != 0
    }

    fn checkpoint_drain_pending(&self) -> bool {
        #[cfg(feature = "cluster")]
        if self.pending_cluster_input.is_some() || self.last_broadcasts != self.local_frontiers {
            return true;
        }
        false
    }

    #[cfg(feature = "cluster")]
    fn deferred_work_is_runnable(&self) -> bool {
        self.queued_remote_events != 0
            || (self.pending_cluster_input.is_none()
                && self.last_broadcasts != self.local_frontiers)
    }

    fn advances_frontier_without_input(&self) -> bool {
        true
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        #[cfg(feature = "cluster")]
        let (prepared, retired) = {
            let prepared = self
                .prepared_vnode_transition
                .as_ref()
                .map_or(0, Self::transition_accounted_bytes);
            let (prepared, retired) = match self.vnode_transition_cleanup.as_ref() {
                Some(IntervalJoinTransitionCleanup::Aborted(transition)) => (
                    prepared.saturating_add(Self::transition_accounted_bytes(transition)),
                    0,
                ),
                Some(IntervalJoinTransitionCleanup::Published(transition)) => {
                    (prepared, Self::transition_accounted_bytes(transition))
                }
                None => (prepared, 0),
            };
            (prepared, retired)
        };
        #[cfg(not(feature = "cluster"))]
        let (prepared, retired) = (0, 0);
        Some(ManagedStateAccountingSnapshot {
            live: self.accounted_state_bytes(),
            prepared,
            retired,
        })
    }

    fn set_managed_state_budget(&mut self, bytes: usize) {
        self.max_managed_state_bytes = bytes;
    }

    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_watermark = watermarks.first().copied().unwrap_or(i64::MIN);
        let right_watermark = watermarks.get(1).copied().unwrap_or(left_watermark);
        self.process_with_frontiers(
            inputs,
            &[
                InputFrontier {
                    watermark: (left_watermark != i64::MIN).then_some(left_watermark),
                    idle: false,
                },
                InputFrontier {
                    watermark: (right_watermark != i64::MIN).then_some(right_watermark),
                    idle: false,
                },
            ],
        )
        .await
    }

    async fn process_with_frontiers(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: &[InputFrontier],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_frontier = frontiers.first().copied().unwrap_or_default();
        let right_frontier = frontiers.get(1).copied().unwrap_or(left_frontier);
        let left_watermark = left_frontier.watermark.unwrap_or(i64::MIN);
        let right_watermark = right_frontier.watermark.unwrap_or(i64::MIN);

        #[cfg(feature = "cluster")]
        if self.cluster_shuffle.is_some() {
            return self
                .process_cluster(inputs, left_frontier, right_frontier)
                .await;
        }

        let mut routed = self.route_local_inputs(inputs)?;
        let frontier_advanced = left_watermark > self.applied_left_watermark
            || right_watermark > self.applied_right_watermark;
        if frontier_advanced {
            self.add_resident_vnodes(&mut routed);
        }
        let output = self.execute_routed_shards(routed, left_watermark, right_watermark)?;
        let output = self.project_output(output).await?;
        self.applied_left_watermark = self.applied_left_watermark.max(left_watermark);
        self.applied_right_watermark = self.applied_right_watermark.max(right_watermark);
        self.applied_left_idle = left_frontier.idle;
        self.applied_right_idle = right_frontier.idle;
        Ok(output)
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let Some(capture) = self.capture_operator_checkpoint(u64::MAX)? else {
            return Ok(None);
        };
        let context = format!(
            "interval join [{}] whole checkpoint serialization",
            self.projection.op_name
        );
        capture
            .encode(self.max_managed_state_bytes, &context)
            .map(|data| Some(OperatorCheckpoint { data }))
    }

    fn checkpoint_capture(
        &mut self,
        max_capture_bytes: u64,
    ) -> Result<Option<StateFrameCapture>, DbError> {
        let Some(capture) = self.capture_operator_checkpoint(max_capture_bytes)? else {
            return Ok(None);
        };
        let retained_bytes = capture.retained_bytes();
        let max_managed_state_bytes = self.max_managed_state_bytes;
        let context = format!(
            "interval join [{}] whole checkpoint serialization",
            self.projection.op_name
        );
        Ok(Some(StateFrameCapture::deferred(
            retained_bytes,
            move |remaining| {
                capture
                    .encode(remaining.min(max_managed_state_bytes), &context)
                    .map(EncodedStateFrame::from_vec)
            },
        )))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        if self.applied_left_watermark != i64::MIN
            || self.applied_right_watermark != i64::MIN
            || self.applied_left_idle
            || self.applied_right_idle
            || {
                #[cfg(feature = "cluster")]
                {
                    self.pending_cluster_input.is_some()
                        || self.queued_remote_events != 0
                        || self.local_frontiers != [InputFrontier::default(); 2]
                        || self.last_broadcasts != [InputFrontier::default(); 2]
                }
                #[cfg(not(feature = "cluster"))]
                {
                    false
                }
            }
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint restore was applied more than once",
                self.projection.op_name
            )));
        }
        if checkpoint.data.len() > self.max_managed_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint payload is {} bytes; restore limit is {} bytes",
                self.projection.op_name,
                checkpoint.data.len(),
                self.max_managed_state_bytes
            )));
        }
        let checkpoint = rkyv::from_bytes::<IntervalJoinOperatorCheckpoint, rkyv::rancor::Error>(
            &checkpoint.data,
        )
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "interval join [{}] checkpoint deserialization: {error}",
                self.projection.op_name
            ))
        })?;
        self.validate_checkpoint_config(&checkpoint)?;
        #[cfg(feature = "cluster")]
        let decoded_cluster = match (self.cluster_shuffle.clone(), checkpoint.cluster) {
            (Some(config), Some(cluster)) => {
                let (_, assignment, peers) = self.active_cluster_scope()?;
                let expected = self.checkpoint_assignment_identity()?;
                if cluster.assignment_version != expected.0
                    || cluster.owner_map_digest != expected.1
                    || cluster.self_id != expected.2
                    || cluster.recovery_gen > config.receiver.recovery_gen()
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] checkpoint assignment or recovery does not match the restored operator",
                        self.projection.op_name
                    )));
                }
                let local_frontiers = cluster.local_frontiers.map(Into::into);
                for side in [JoinInputSide::Left, JoinInputSide::Right] {
                    self.validate_frontier(
                        InputFrontier::default(),
                        local_frontiers[side.port()],
                        side,
                    )?;
                }
                if cluster.remote_side_cursor > 1 {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] checkpoint side cursor is invalid",
                        self.projection.op_name
                    )));
                }
                let mut peer_channels = [BTreeMap::new(), BTreeMap::new()];
                let mut queued_bytes = 0usize;
                let mut queued_events = 0usize;
                let mut capacity_bytes = 0usize;
                for side in [JoinInputSide::Left, JoinInputSide::Right] {
                    let port = side.port();
                    if cluster.channels[port].len() != peers.len()
                        || !cluster.channels[port]
                            .iter()
                            .map(|channel| channel.peer)
                            .eq(peers.iter().copied())
                    {
                        return Err(DbError::Checkpoint(format!(
                            "interval join [{}] {} checkpoint channel roster is invalid",
                            self.projection.op_name,
                            side.name()
                        )));
                    }
                    for channel in &cluster.channels[port] {
                        let applied: InputFrontier = channel.applied.into();
                        self.validate_frontier(InputFrontier::default(), applied, side)?;
                        let mut runtime = IntervalPeerChannel {
                            applied,
                            accepted: applied,
                            events: VecDeque::new(),
                        };
                        runtime
                            .events
                            .try_reserve_exact(channel.events.len())
                            .map_err(|error| {
                                DbError::Checkpoint(format!(
                                    "interval join [{}] peer {} restore queue reservation: {error}",
                                    self.projection.op_name, channel.peer
                                ))
                            })?;
                        capacity_bytes = capacity_bytes
                            .checked_add(
                                runtime
                                    .events
                                    .capacity()
                                    .checked_mul(REMOTE_EVENT_CHARGE)
                                    .ok_or_else(|| self.accounting_error())?,
                            )
                            .ok_or_else(|| self.accounting_error())?;
                        let mut previous_recovery = None;
                        for event in &channel.events {
                            let payload = match event {
                                IntervalCheckpointEvent::Data {
                                    recovery_gen,
                                    routed_vnodes,
                                    ipc,
                                } => {
                                    if runtime.accepted.idle
                                        || *recovery_gen > cluster.recovery_gen
                                        || previous_recovery
                                            .is_some_and(|previous| *recovery_gen < previous)
                                        || routed_vnodes.is_empty()
                                    {
                                        return Err(DbError::Checkpoint(format!(
                                            "interval join [{}] peer {} restored data is behind an idle or invalid channel",
                                            self.projection.op_name, channel.peer
                                        )));
                                    }
                                    let batch = laminar_core::serialization::deserialize_batch_stream(ipc)
                                        .map_err(|error| DbError::Checkpoint(format!(
                                            "interval join [{}] peer {} queued data restore: {error}",
                                            self.projection.op_name, channel.peer
                                        )))?;
                                    let retained = crate::operator::RetainedBatch::restored_channel(
                                        batch,
                                        channel.peer,
                                        cluster.assignment_version,
                                        *recovery_gen,
                                        Arc::from(routed_vnodes.clone()),
                                    );
                                    let batch = self.build_queued_batch(
                                        retained,
                                        runtime.accepted,
                                        &config,
                                        &assignment,
                                        side,
                                    )?;
                                    queued_bytes = queued_bytes
                                        .checked_add(batch.charged_bytes)
                                        .ok_or_else(|| self.accounting_error())?;
                                    IntervalRemoteEventPayload::Data(batch)
                                }
                                IntervalCheckpointEvent::Frontier {
                                    recovery_gen,
                                    frontier,
                                } => {
                                    if *recovery_gen > cluster.recovery_gen
                                        || previous_recovery
                                            .is_some_and(|previous| *recovery_gen < previous)
                                    {
                                        return Err(DbError::Checkpoint(format!(
                                            "interval join [{}] peer {} frontier recovery is invalid",
                                            self.projection.op_name, channel.peer
                                        )));
                                    }
                                    let frontier: InputFrontier = (*frontier).into();
                                    let floor = if port == 0 {
                                        checkpoint.applied_left_watermark
                                    } else {
                                        checkpoint.applied_right_watermark
                                    };
                                    if runtime.accepted.idle
                                        && !frontier.idle
                                        && Self::watermark_option(floor).is_some_and(|floor| {
                                            frontier
                                                .watermark
                                                .is_none_or(|watermark| watermark < floor)
                                        })
                                    {
                                        return Err(DbError::Checkpoint(format!(
                                            "interval join [{}] peer {} {} revival frontier is below its checkpoint floor",
                                            self.projection.op_name,
                                            channel.peer,
                                            side.name()
                                        )));
                                    }
                                    self.validate_frontier(runtime.accepted, frontier, side)?;
                                    runtime.accepted = frontier;
                                    IntervalRemoteEventPayload::Frontier(frontier)
                                }
                            };
                            let recovery_gen = match event {
                                IntervalCheckpointEvent::Data { recovery_gen, .. }
                                | IntervalCheckpointEvent::Frontier { recovery_gen, .. } => {
                                    *recovery_gen
                                }
                            };
                            previous_recovery = Some(recovery_gen);
                            runtime.events.push_back(IntervalRemoteEvent {
                                assignment_version: cluster.assignment_version,
                                recovery_gen,
                                payload,
                            });
                            queued_events = queued_events
                                .checked_add(1)
                                .ok_or_else(|| self.accounting_error())?;
                        }
                        peer_channels[port].insert(channel.peer, runtime);
                    }
                }
                for port in 0..2 {
                    if cluster.remote_peer_cursors[port]
                        .is_some_and(|peer| peers.binary_search(&peer).is_err())
                    {
                        return Err(DbError::Checkpoint(format!(
                            "interval join [{}] checkpoint remote cursor is invalid",
                            self.projection.op_name
                        )));
                    }
                    let merged = merge_input_frontier_iter(
                        std::iter::once(local_frontiers[port]).chain(
                            peer_channels[port].values().map(|channel| {
                                let mut applied = channel.applied;
                                if !channel.events.is_empty() {
                                    applied.idle = false;
                                    let floor = if port == 0 {
                                        checkpoint.applied_left_watermark
                                    } else {
                                        checkpoint.applied_right_watermark
                                    };
                                    applied.watermark = Self::max_watermark(
                                        applied.watermark,
                                        Self::watermark_option(floor),
                                    );
                                }
                                applied
                            }),
                        ),
                        i64::MIN,
                    );
                    let expected_watermark = if port == 0 {
                        checkpoint.applied_left_watermark
                    } else {
                        checkpoint.applied_right_watermark
                    };
                    let expected_idle = if port == 0 {
                        checkpoint.applied_left_idle
                    } else {
                        checkpoint.applied_right_idle
                    };
                    let queue_empty = peer_channels[port]
                        .values()
                        .all(|channel| channel.events.is_empty());
                    if merged.watermark.unwrap_or(i64::MIN) != expected_watermark
                        || (queue_empty && merged.idle != expected_idle)
                    {
                        return Err(DbError::Checkpoint(format!(
                            "interval join [{}] restored cluster frontier is inconsistent",
                            self.projection.op_name
                        )));
                    }
                }
                Some((
                    local_frontiers,
                    peer_channels,
                    cluster.remote_side_cursor,
                    cluster.remote_peer_cursors,
                    queued_bytes,
                    queued_events,
                    capacity_bytes,
                ))
            }
            (None, None) => None,
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] checkpoint deployment mode does not match the operator",
                    self.projection.op_name
                )));
            }
        };
        #[cfg(not(feature = "cluster"))]
        if checkpoint.cluster.is_some() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint contains cluster channel state",
                self.projection.op_name
            )));
        }

        #[cfg(feature = "cluster")]
        if let Some((_, channels, _, _, queued_bytes, _, capacity_bytes)) = decoded_cluster.as_ref()
        {
            let restored_channels = channels
                .iter()
                .map(BTreeMap::len)
                .sum::<usize>()
                .checked_mul(
                    std::mem::size_of::<(u64, IntervalPeerChannel)>()
                        .saturating_add(PEER_CHANNEL_ENTRY_CHARGE),
                )
                .ok_or_else(|| self.accounting_error())?;
            let restored_cluster = self
                .cluster_peers
                .len()
                .checked_mul(std::mem::size_of::<u64>())
                .and_then(|bytes| bytes.checked_add(restored_channels))
                .and_then(|bytes| bytes.checked_add(*capacity_bytes))
                .and_then(|bytes| bytes.checked_add(*queued_bytes))
                .ok_or_else(|| self.accounting_error())?;
            let projected_accounted = self
                .accounted_state_bytes()
                .checked_sub(self.cluster_accounted_bytes())
                .and_then(|bytes| bytes.checked_add(restored_cluster))
                .ok_or_else(|| self.accounting_error())?;
            if projected_accounted > self.max_managed_state_bytes {
                return Err(DbError::ManagedStateBudgetExceeded {
                    context: format!(
                        "interval join [{}] cluster checkpoint restore",
                        self.projection.op_name
                    ),
                    accounted_bytes: projected_accounted,
                    limit_bytes: self.max_managed_state_bytes,
                });
            }
        }

        self.applied_left_watermark = checkpoint.applied_left_watermark;
        self.applied_right_watermark = checkpoint.applied_right_watermark;
        self.applied_left_idle = checkpoint.applied_left_idle;
        self.applied_right_idle = checkpoint.applied_right_idle;
        #[cfg(feature = "cluster")]
        if let Some((local, channels, side_cursor, cursors, bytes, events, capacity)) =
            decoded_cluster
        {
            self.local_frontiers = local;
            self.last_broadcasts = local;
            self.peer_channels = channels;
            self.remote_side_cursor = side_cursor;
            self.remote_peer_cursors = cursors;
            self.queued_shuffle_bytes = bytes;
            self.queued_remote_events = events;
            self.queued_event_capacity_bytes = capacity;
        }

        Ok(())
    }

    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).unwrap_or(i64::MAX);
        let right_only = matches!(
            self.config.join_type,
            laminar_sql::parser::join_parser::JoinType::RightSemi
                | laminar_sql::parser::join_parser::JoinType::RightAnti
        );
        let safe = if right_only {
            self.applied_left_watermark
                .min(self.applied_right_watermark)
        } else {
            self.applied_left_watermark
                .min(self.applied_right_watermark.saturating_sub(bound_ms))
        };
        let mut output = input.with_watermark_ceiling(Some(safe));
        output.idle = self.applied_left_idle && self.applied_right_idle;
        #[cfg(feature = "cluster")]
        if self.pending_cluster_input.is_some() || self.queued_remote_events != 0 {
            output.idle = false;
        }
        output
    }

    #[cfg(feature = "cluster")]
    fn restored_output_frontier(&self) -> Option<InputFrontier> {
        Some(self.output_frontier(InputFrontier {
            watermark: Some(i64::MAX),
            idle: false,
        }))
    }

    #[cfg(feature = "cluster")]
    fn wants_input(&self) -> bool {
        self.pending_cluster_input.is_none()
            && self.queued_remote_events == 0
            && self.last_broadcasts == self.local_frontiers
    }

    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle(
        &mut self,
        stage: &str,
        batch: crate::operator::RetainedBatch,
        _watermark: i64,
    ) -> Result<(), DbError> {
        let side = self.side_for_stage(stage)?;
        let (config, assignment, peers) = self.active_cluster_scope()?;
        let peer = batch.peer().ok_or_else(|| {
            DbError::ShuffleTerminal(format!(
                "interval join [{}] received unscoped shuffle data",
                self.projection.op_name
            ))
        })?;
        if peers.binary_search(&peer).is_err()
            || batch.assignment_version() != Some(assignment.version())
            || batch.recovery_gen() != Some(config.receiver.recovery_gen())
        {
            return Err(DbError::ShuffleTerminal(format!(
                "interval join [{}] received {} data outside assignment {} recovery {}",
                self.projection.op_name,
                side.name(),
                assignment.version(),
                config.receiver.recovery_gen()
            )));
        }
        let accepted = self.peer_channels[side.port()]
            .get(&peer)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] has no {} channel for peer {peer}",
                    self.projection.op_name,
                    side.name()
                ))
            })?
            .accepted;
        let batch = self.build_queued_batch(batch, accepted, &config, &assignment, side)?;
        let charged_bytes = batch.charged_bytes;
        let (next_bytes, next_events) =
            self.reserve_remote_event_slot(side, peer, charged_bytes)?;
        let assignment_version = batch
            .retained
            .assignment_version()
            .expect("validated interval assignment");
        let recovery_gen = batch
            .retained
            .recovery_gen()
            .expect("validated interval recovery");
        self.queued_shuffle_bytes = next_bytes;
        self.queued_remote_events = next_events;
        self.peer_channels[side.port()]
            .get_mut(&peer)
            .expect("reserved interval peer channel")
            .events
            .push_back(IntervalRemoteEvent {
                assignment_version,
                recovery_gen,
                payload: IntervalRemoteEventPayload::Data(batch),
            });
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle_frontier(
        &mut self,
        stage: &str,
        peer: u64,
        mut frontier: InputFrontier,
        assignment_version: u64,
        recovery_gen: u64,
    ) -> Result<(), DbError> {
        let side = self.side_for_stage(stage)?;
        let (config, assignment, peers) = self.active_cluster_scope()?;
        if peers.binary_search(&peer).is_err()
            || assignment_version != assignment.version()
            || recovery_gen != config.receiver.recovery_gen()
            || frontier.watermark == Some(i64::MIN)
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] received {} frontier from peer {peer} outside assignment {} recovery {}",
                self.projection.op_name,
                side.name(),
                assignment.version(),
                config.receiver.recovery_gen()
            )));
        }
        let previous = self.peer_channels[side.port()][&peer].accepted;
        self.validate_frontier(previous, frontier, side)?;
        if previous.idle && !frontier.idle {
            let floor = if side.port() == 0 {
                self.applied_left_watermark
            } else {
                self.applied_right_watermark
            };
            frontier.watermark =
                Self::max_watermark(frontier.watermark, Self::watermark_option(floor));
        }
        let (next_bytes, next_events) = self.reserve_remote_event_slot(side, peer, 0)?;
        self.queued_shuffle_bytes = next_bytes;
        self.queued_remote_events = next_events;
        let channel = self.peer_channels[side.port()]
            .get_mut(&peer)
            .expect("reserved interval peer channel");
        channel.events.push_back(IntervalRemoteEvent {
            assignment_version,
            recovery_gen,
            payload: IntervalRemoteEventPayload::Frontier(frontier),
        });
        channel.accepted = frontier;
        Ok(())
    }

    fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
        max_capture_bytes: u64,
    ) -> Result<Option<Vec<CapturedVnodeState>>, DbError> {
        #[cfg(feature = "cluster")]
        if self.pending_cluster_input.is_some() || self.last_broadcasts != self.local_frontiers {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] vnode capture requires a drained channel cut",
                self.projection.op_name
            )));
        }
        if u32::from(self.key_group_count) != vnode_count
            || required_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
            || required_vnodes.iter().any(|vnode| *vnode >= vnode_count)
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] received a non-canonical vnode roster {required_vnodes:?} for vnode_count {vnode_count}",
                self.projection.op_name
            )));
        }
        if let Some(unowned) = self
            .resident_vnodes
            .iter()
            .copied()
            .find(|vnode| required_vnodes.binary_search(vnode).is_err())
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] retained unowned vnode state {unowned}",
                self.projection.op_name
            )));
        }

        let full_capture = self.full_vnode_capture_required;
        let mut capture_vnodes = if full_capture {
            required_vnodes.to_vec()
        } else {
            self.dirty_vnode_roster
                .iter()
                .copied()
                .filter(|vnode| required_vnodes.binary_search(vnode).is_ok())
                .collect::<Vec<_>>()
        };
        if !full_capture {
            capture_vnodes.sort_unstable();
        }
        debug_assert!(capture_vnodes.windows(2).all(|pair| pair[0] < pair[1]));
        if capture_vnodes.is_empty() {
            self.full_vnode_capture_required = false;
            self.clear_dirty_vnode_roster();
            return Ok(Some(Vec::new()));
        }

        let absent_frame_bytes = capture_vnodes
            .iter()
            .filter(|vnode| self.vnode_states[**vnode as usize].is_none())
            .count()
            .checked_mul(VNODE_FRAME_HEADER_LEN)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] absent vnode frame accounting overflow",
                    self.projection.op_name
                ))
            })?;
        let mut captured = Vec::with_capacity(capture_vnodes.len());
        let mut retained_capture_bytes = 0_u64;
        let operator_remaining = Arc::new(AtomicUsize::new(
            self.max_managed_state_bytes
                .checked_sub(absent_frame_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] absent vnode frames exceed their {}-byte checkpoint limit",
                        self.projection.op_name, self.max_managed_state_bytes
                    ))
                })?,
        ));
        for vnode in capture_vnodes {
            let state = if let Some(state) = self.vnode_states[vnode as usize].as_ref() {
                let remaining_capture_bytes = max_capture_bytes
                    .checked_sub(retained_capture_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] checkpoint captures exhausted their {max_capture_bytes}-byte capture budget",
                            self.projection.op_name
                        ))
                    })?;
                let state_bytes = u64::try_from(state.accounted_state_bytes()).unwrap_or(u64::MAX);
                if state_bytes > remaining_capture_bytes {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] vnode {vnode} retains {state_bytes} bytes; remaining capture budget is {remaining_capture_bytes} bytes",
                        self.projection.op_name
                    )));
                }
                let state_capture = state.capture_checkpoint(&self.config)?;
                let retained_bytes =
                    u64::try_from(state_capture.retained_bytes()).unwrap_or(u64::MAX);
                retained_capture_bytes = retained_capture_bytes
                    .checked_add(retained_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] checkpoint capture byte accounting overflow",
                            self.projection.op_name
                        ))
                    })?;
                if retained_capture_bytes > max_capture_bytes {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] checkpoint captures retain {retained_capture_bytes} bytes; capture budget is {max_capture_bytes} bytes",
                        self.projection.op_name
                    )));
                }
                let max_managed_state_bytes = self.max_managed_state_bytes;
                let operator_remaining = Arc::clone(&operator_remaining);
                let context = format!(
                    "interval join [{}] vnode {vnode} checkpoint serialization",
                    self.projection.op_name
                );
                Some(StateFrameCapture::deferred(
                    retained_bytes,
                    move |remaining| {
                        let logical_remaining = operator_remaining.load(Ordering::Relaxed);
                        let limit = remaining
                            .min(max_managed_state_bytes)
                            .min(logical_remaining);
                        let encoded = Self::encode_state_capture(state_capture, &context, limit)?;
                        operator_remaining
                            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                                remaining.checked_sub(encoded.payload_len())
                            })
                            .map_err(|_| {
                                DbError::Checkpoint(format!(
                                    "{context}: vnode checkpoints exhausted their {max_managed_state_bytes}-byte limit"
                                ))
                            })?;
                        Ok(encoded)
                    },
                ))
            } else {
                Some(StateFrameCapture::encoded_static(&ABSENT_VNODE_FRAME))
            };
            captured.push(CapturedVnodeState { vnode, state });
        }

        self.full_vnode_capture_required = false;
        self.clear_dirty_vnode_roster();
        Ok(Some(captured))
    }

    fn restore_vnode(&mut self, vnode: u32, vnode_count: u32, state: &[u8]) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        if self.pending_cluster_input.is_some() || self.last_broadcasts != self.local_frontiers {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] vnode restore requires a pristine channel cut",
                self.projection.op_name
            )));
        }
        if u32::from(self.key_group_count) != vnode_count || vnode >= vnode_count {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] vnode {vnode} restore does not match its {vnode_count}-vnode topology",
                self.projection.op_name
            )));
        }
        let current_bytes = self.vnode_states[vnode as usize]
            .as_ref()
            .map_or(0, |state| state.accounted_state_bytes());
        let other_bytes = self.accounted_state_bytes().saturating_sub(current_bytes);
        let remaining = self
            .max_managed_state_bytes
            .checked_sub(other_bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] restored state exceeds its {}-byte limit",
                    self.projection.op_name, self.max_managed_state_bytes
                ))
            })?;
        let replacement = Self::decode_vnode_frame(
            state,
            &self.config,
            &format!(
                "interval join [{}] vnode {vnode} restore",
                self.projection.op_name
            ),
            remaining,
            None,
        )?;
        self.vnode_states[vnode as usize] = if let Some(mut replacement) = replacement {
            replacement.validate_vnode(vnode, vnode_count, &self.config)?;
            if let Some((left_schema, right_schema)) = &self.input_schemas {
                replacement.seed_input_schemas(
                    left_schema.clone(),
                    right_schema.clone(),
                    &self.config,
                )?;
            }
            Some(Box::new(replacement))
        } else {
            None
        };
        if self.vnode_states[vnode as usize].is_some() {
            self.add_resident_vnode(vnode);
        } else {
            self.remove_resident_vnode(vnode);
        }
        self.mark_vnode_dirty(vnode);
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn prepare_vnode_transition(
        &mut self,
        transition: ManagedVnodeTransition<'_>,
    ) -> Result<(), DbError> {
        if self.prepared_vnode_transition.is_some() || self.vnode_transition_cleanup.is_some() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] already owns vnode transition state",
                self.projection.op_name
            )));
        }
        if self.pending_cluster_input.is_some()
            || self.queued_remote_events != 0
            || self.queued_shuffle_bytes != 0
            || self.last_broadcasts != self.local_frontiers
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] transition requires a drained channel cut",
                self.projection.op_name
            )));
        }
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] cannot transition without cluster ownership",
                self.projection.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        let installed_owners: Vec<u64> = self
            .local_assignment
            .owners()
            .iter()
            .map(|owner| owner.0)
            .collect();
        let checkpoint_bootstrap = match transition.mode {
            ManagedVnodeTransitionMode::Live => false,
            ManagedVnodeTransitionMode::CheckpointBootstrap { predecessor_owners } => {
                let predecessor_owner_ids = predecessor_owners
                    .iter()
                    .map(|owner| owner.0)
                    .collect::<Vec<_>>();
                if !transition
                    .predecessor
                    .matches_owner_map(&predecessor_owner_ids)
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] checkpoint bootstrap has an invalid predecessor owner map",
                        self.projection.op_name
                    )));
                }
                true
            }
        };
        let target_contains_self = assignment.owners().contains(&config.self_id);
        let endpoints_match_process = config.sender.local_id() == config.self_id.0
            && config.receiver.local_id() == config.self_id.0
            && config.sender.incarnation() == config.receiver.incarnation();
        let active_transport = endpoints_match_process
            && config.sender.assignment_version() == assignment.version()
            && config.receiver.assignment_version() == assignment.version()
            && config.sender.active_assignment_digest() == Some(transition.target.digest())
            && config.receiver.active_assignment_digest()
                == config.sender.active_assignment_digest()
            && transition.target.participant_incarnation(config.self_id.0)
                == Some(config.sender.incarnation());
        let inactive_transport = endpoints_match_process
            && config.sender.assignment_version() == 0
            && config.receiver.assignment_version() == 0
            && config.sender.active_assignment_digest().is_none()
            && config.receiver.active_assignment_digest().is_none()
            && !transition.target.contains(config.self_id.0);
        if transition.target.vnode_count != config.registry.vnode_count()
            || transition.target.assignment_version != assignment.version()
            || !transition.target.matches_owner_map(&owners)
            || target_contains_self != transition.target.contains(config.self_id.0)
            || (target_contains_self && !active_transport)
            || (!target_contains_self && !inactive_transport)
            || transition.predecessor.assignment_version.checked_add(1)
                != Some(transition.target.assignment_version)
            || if checkpoint_bootstrap {
                self.local_assignment.version() != assignment.version()
                    || self.local_assignment.owners() != assignment.owners()
                    || self.vnode_states.iter().any(Option::is_some)
                    || self.applied_left_watermark != i64::MIN
                    || self.applied_right_watermark != i64::MIN
                    || self.applied_left_idle
                    || self.applied_right_idle
                    || self.local_frontiers != [InputFrontier::default(); 2]
                    || self.last_broadcasts != [InputFrontier::default(); 2]
            } else {
                transition.predecessor.assignment_version != self.local_assignment.version()
                    || !transition.predecessor.matches_owner_map(&installed_owners)
            }
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] transition target does not match assignment {}",
                self.projection.op_name,
                assignment.version()
            )));
        }
        let fresh_acquirer = target_contains_self
            && (checkpoint_bootstrap
                || transition
                    .predecessor
                    .participant_incarnation(config.self_id.0)
                    != Some(config.sender.incarnation()));
        let predecessor_peers = Self::remote_owner_peers(&self.local_assignment, config.self_id);
        if !fresh_acquirer
            && (self.cluster_peers.as_ref() != predecessor_peers.as_slice()
                || self.peer_channels.iter().any(|channels| {
                    channels.len() != predecessor_peers.len()
                        || !channels
                            .keys()
                            .copied()
                            .eq(predecessor_peers.iter().copied())
                        || channels.values().any(|channel| {
                            !channel.events.is_empty() || channel.accepted != channel.applied
                        })
                }))
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] transition found stale predecessor channels",
                self.projection.op_name
            )));
        }

        if fresh_acquirer {
            let target_owned = assignment
                .owners()
                .iter()
                .zip(0_u32..)
                .filter_map(|(owner, vnode)| (*owner == config.self_id).then_some(vnode))
                .collect::<Vec<_>>();
            let restored = transition
                .restores
                .iter()
                .map(|restore| restore.vnode)
                .collect::<Vec<_>>();
            if !transition.revoked.is_empty()
                || target_owned.is_empty()
                || restored != target_owned
                || restored.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] fresh-owner transition does not contain every acquired vnode frame",
                    self.projection.op_name
                )));
            }
        }
        let pristine_restore_target = !transition.restores.is_empty()
            && self.vnode_states.iter().all(Option::is_none)
            && self.applied_left_watermark == i64::MIN
            && self.applied_right_watermark == i64::MIN
            && !self.applied_left_idle
            && !self.applied_right_idle
            && self.local_frontiers == [InputFrontier::default(); 2]
            && self.last_broadcasts == [InputFrontier::default(); 2]
            && self.queued_remote_events == 0
            && self.pending_cluster_input.is_none();
        let requires_handoff_cut = fresh_acquirer || pristine_restore_target;
        let mut handoff_cut = self.portable_handoff_cut(&transition, requires_handoff_cut)?;
        let derive_handoff_cut = requires_handoff_cut && handoff_cut.is_none();
        let restore_cut = if transition.restores.is_empty() || derive_handoff_cut {
            None
        } else {
            Some(handoff_cut.unwrap_or(IntervalHandoffCut {
                left_watermark: self.applied_left_watermark,
                right_watermark: self.applied_right_watermark,
                left_idle: self.applied_left_idle,
                right_idle: self.applied_right_idle,
            }))
        };

        if self.vnode_states.len() != transition.target.vnode_count as usize {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] state table has {} slots for target vnode count {}",
                self.projection.op_name,
                self.vnode_states.len(),
                transition.target.vnode_count
            )));
        }

        let mut replacements = BTreeMap::new();
        for vnode in transition.revoked {
            if *vnode >= transition.target.vnode_count {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] revoked vnode {vnode} is outside the target vnode space",
                    self.projection.op_name
                )));
            }
            replacements.insert(*vnode, None);
        }
        let mut restore_vnodes = rustc_hash::FxHashSet::default();
        let mut restore_payload_bytes = 0usize;
        for restore in transition.restores {
            if !restore_vnodes.insert(restore.vnode) {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] transition repeats restored vnode {}",
                    self.projection.op_name, restore.vnode
                )));
            }
            if assignment
                .owners()
                .get(usize::try_from(restore.vnode).unwrap_or(usize::MAX))
                != Some(&config.self_id)
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] restore vnode {} is not owned by this node",
                    self.projection.op_name, restore.vnode
                )));
            }
            restore_payload_bytes = restore_payload_bytes
                .checked_add(restore.state.len())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] transition restore payload accounting overflow",
                        self.projection.op_name
                    ))
                })?;
            if restore_payload_bytes > self.max_managed_state_bytes {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] transition restore payload exceeds its {}-byte limit",
                    self.projection.op_name, self.max_managed_state_bytes
                )));
            }
        }

        let live_bytes = self.accounted_state_bytes();
        let mut restored_bytes = 0usize;
        let mut recovered_cut: Option<IntervalHandoffCut> = None;
        for restore in transition.restores {
            let remaining = self
                .max_managed_state_bytes
                .checked_sub(
                    live_bytes
                        .saturating_add(restore_payload_bytes)
                        .saturating_add(restored_bytes),
                )
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] transition state exceeds its {}-byte limit",
                        self.projection.op_name, self.max_managed_state_bytes
                    ))
                })?;
            let context = format!(
                "interval join [{}] vnode {} restore",
                self.projection.op_name, restore.vnode
            );
            let Some(mut state) = Self::decode_vnode_frame(
                restore.state,
                &self.config,
                &context,
                remaining,
                restore_cut,
            )?
            else {
                replacements.insert(restore.vnode, None);
                continue;
            };
            if derive_handoff_cut {
                if let Some(candidate) = self.restored_handoff_cut_evidence(&state, &context)? {
                    if recovered_cut.is_some_and(|expected| {
                        expected.left_watermark != candidate.left_watermark
                            || expected.right_watermark != candidate.right_watermark
                    }) {
                        return Err(DbError::Checkpoint(format!(
                            "interval join [{}] restored vnode frames disagree on the handoff watermarks",
                            self.projection.op_name
                        )));
                    }
                    recovered_cut = Some(candidate);
                }
            }
            if let Some((left_schema, right_schema)) = &self.input_schemas {
                state.seed_input_schemas(
                    left_schema.clone(),
                    right_schema.clone(),
                    &self.config,
                )?;
            }
            state.validate_vnode(restore.vnode, transition.target.vnode_count, &self.config)?;
            restored_bytes = restored_bytes.saturating_add(state.accounted_state_bytes());
            replacements.insert(restore.vnode, Some(Box::new(state)));
        }
        if derive_handoff_cut {
            let cut = recovered_cut.unwrap_or(IntervalHandoffCut {
                left_watermark: i64::MIN,
                right_watermark: i64::MIN,
                left_idle: false,
                right_idle: false,
            });
            for (vnode, state) in replacements
                .iter()
                .filter_map(|(vnode, state)| state.as_deref().map(|state| (*vnode, state)))
            {
                let (left_rows, right_rows) = state.buffered_rows();
                let (left_evicted_cutoff, right_evicted_cutoff) = state.evicted_cutoffs();
                Self::validate_handoff_cutoffs(
                    left_evicted_cutoff,
                    right_evicted_cutoff,
                    left_rows != 0,
                    right_rows != 0,
                    &self.config,
                    &format!(
                        "interval join [{}] vnode {vnode} restore",
                        self.projection.op_name
                    ),
                    cut,
                )?;
            }
            handoff_cut = Some(cut);
        }
        for ((state, owner), vnode) in self
            .vnode_states
            .iter()
            .zip(assignment.owners())
            .zip(0_u32..)
        {
            if state.is_none() || replacements.contains_key(&vnode) {
                continue;
            }
            if owner != &config.self_id {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] transition retained unowned vnode {vnode}",
                    self.projection.op_name
                )));
            }
        }
        let target_resident_capacity = self.resident_vnodes.len().saturating_add(
            replacements
                .values()
                .filter(|state| state.is_some())
                .count(),
        );
        let mut resident_vnodes = Vec::new();
        resident_vnodes
            .try_reserve_exact(target_resident_capacity)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "interval join [{}] target resident-vnode roster reservation failed: {error}",
                    self.projection.op_name
                ))
            })?;
        let mut resident_index = 0;
        for (&vnode, replacement) in &replacements {
            while self
                .resident_vnodes
                .get(resident_index)
                .is_some_and(|resident| *resident < vnode)
            {
                resident_vnodes.push(self.resident_vnodes[resident_index]);
                resident_index += 1;
            }
            if self.resident_vnodes.get(resident_index) == Some(&vnode) {
                resident_index += 1;
            }
            if replacement.is_some() {
                resident_vnodes.push(vnode);
            }
        }
        resident_vnodes.extend_from_slice(&self.resident_vnodes[resident_index..]);
        let target_peers = Self::remote_owner_peers(&assignment, config.self_id);
        let transition_frontiers = handoff_cut.map_or(self.applied_frontiers(), |cut| {
            [
                InputFrontier {
                    watermark: Self::watermark_option(cut.left_watermark),
                    idle: cut.left_idle,
                },
                InputFrontier {
                    watermark: Self::watermark_option(cut.right_watermark),
                    idle: cut.right_idle,
                },
            ]
        });
        let peer_incarnation_changed = target_peers.iter().copied().any(|peer| {
            transition.predecessor.participant_incarnation(peer)
                != transition.target.participant_incarnation(peer)
        });
        let mut peer_channels = [BTreeMap::new(), BTreeMap::new()];
        for port in 0..2 {
            for &peer in &target_peers {
                let same_incarnation = transition.predecessor.participant_incarnation(peer)
                    == transition.target.participant_incarnation(peer);
                let applied = if fresh_acquirer || !same_incarnation {
                    transition_frontiers[port]
                } else {
                    self.peer_channels[port]
                        .get(&peer)
                        .map_or(transition_frontiers[port], |channel| channel.applied)
                };
                peer_channels[port].insert(
                    peer,
                    IntervalPeerChannel {
                        applied,
                        accepted: applied,
                        events: VecDeque::new(),
                    },
                );
            }
        }
        let bootstrap_broadcast = !target_peers.is_empty()
            && (fresh_acquirer
                || peer_incarnation_changed
                || target_peers.as_slice() != self.cluster_peers.as_ref());
        let prepared = PreparedIntervalJoinTransition {
            replacements: replacements.into_iter().collect(),
            local_assignment: assignment,
            resident_vnodes,
            cluster_peers: target_peers.into(),
            peer_channels,
            bootstrap_broadcast,
            handoff_cut,
        };
        let total_bytes = live_bytes
            .saturating_add(restore_payload_bytes)
            .saturating_add(Self::transition_accounted_bytes(&prepared));
        if total_bytes > self.max_managed_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] prepared transition accounts {total_bytes} bytes; limit is {} bytes",
                self.projection.op_name, self.max_managed_state_bytes
            )));
        }
        self.prepared_vnode_transition = Some(prepared);
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn abort_vnode_transition(&mut self) {
        let Some(prepared) = self.prepared_vnode_transition.take() else {
            return;
        };
        assert!(self.vnode_transition_cleanup.is_none());
        self.vnode_transition_cleanup = Some(IntervalJoinTransitionCleanup::Aborted(prepared));
    }

    #[cfg(feature = "cluster")]
    fn publish_vnode_transition(&mut self) {
        let prepared = self
            .prepared_vnode_transition
            .take()
            .expect("interval join transition must be prepared before publication");
        assert!(self.vnode_transition_cleanup.is_none());
        let mut prepared = prepared;
        for (vnode, replacement) in &mut prepared.replacements {
            let slot = self
                .vnode_states
                .get_mut(*vnode as usize)
                .expect("prepared interval join vnode must have a state slot");
            std::mem::swap(slot, replacement);
            self.mark_vnode_dirty(*vnode);
        }
        std::mem::swap(&mut self.local_assignment, &mut prepared.local_assignment);
        std::mem::swap(&mut self.resident_vnodes, &mut prepared.resident_vnodes);
        std::mem::swap(&mut self.cluster_peers, &mut prepared.cluster_peers);
        std::mem::swap(&mut self.peer_channels, &mut prepared.peer_channels);
        self.remote_side_cursor = 0;
        self.remote_peer_cursors = [None; 2];
        self.queued_event_capacity_bytes = self
            .peer_channels
            .iter()
            .flat_map(BTreeMap::values)
            .map(|channel| {
                channel
                    .events
                    .capacity()
                    .saturating_mul(REMOTE_EVENT_CHARGE)
            })
            .sum();
        if let Some(mut cut) = prepared.handoff_cut {
            std::mem::swap(&mut self.applied_left_watermark, &mut cut.left_watermark);
            std::mem::swap(&mut self.applied_right_watermark, &mut cut.right_watermark);
            std::mem::swap(&mut self.applied_left_idle, &mut cut.left_idle);
            std::mem::swap(&mut self.applied_right_idle, &mut cut.right_idle);
            self.local_frontiers = self.applied_frontiers();
            prepared.handoff_cut = Some(cut);
        }
        self.last_broadcasts = if prepared.bootstrap_broadcast {
            [InputFrontier::default(); 2]
        } else {
            self.local_frontiers
        };
        self.vnode_transition_cleanup = Some(IntervalJoinTransitionCleanup::Published(prepared));
    }

    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {
        self.vnode_transition_cleanup = None;
    }

    fn force_full_vnode_capture(&mut self) {
        self.full_vnode_capture_required = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::time::Duration;

    #[cfg(feature = "cluster")]
    use arrow::array::Int64Array;

    fn materialize_capture(capture: StateFrameCapture) -> Result<bytes::Bytes, DbError> {
        let mut staged_bytes = capture.retained_bytes();
        capture.materialize(&mut staged_bytes, u64::MAX)
    }

    #[cfg(feature = "cluster")]
    async fn single_owner_shuffle(
        vnode_count: u32,
    ) -> (
        ClusterShuffleConfig,
        laminar_core::checkpoint::CheckpointAssignmentFence,
    ) {
        use laminar_core::cluster::control::LeaseDeadline;
        use laminar_core::state::{NodeId, VnodeRegistry};

        let self_id = NodeId(1);
        let incarnation = uuid::Uuid::from_u128(1);
        let registry = Arc::new(VnodeRegistry::single_owner(vnode_count, self_id));
        registry.set_assignment(vec![self_id; vnode_count as usize].into());
        let receiver = Arc::new(
            laminar_core::shuffle::ShuffleReceiver::bind(
                self_id.0,
                "127.0.0.1:0".parse().unwrap(),
                incarnation,
            )
            .await
            .unwrap(),
        );
        let sender = Arc::new(laminar_core::shuffle::ShuffleSender::new(
            self_id.0,
            incarnation,
        ));
        let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
        receiver
            .install_process_lease_deadline(Arc::clone(&deadline))
            .unwrap();
        sender.install_process_lease_deadline(deadline).unwrap();

        let owners = vec![self_id.0; usize::try_from(vnode_count).unwrap()];
        let fence = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
            registry.assignment_version(),
            &owners,
            vec![laminar_core::checkpoint::CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: incarnation,
            }],
        )
        .unwrap();
        sender.install_assignment_fence(&fence, &owners).unwrap();
        receiver.install_assignment_fence(&fence, &owners).unwrap();

        (
            ClusterShuffleConfig {
                registry,
                sender,
                receiver,
                self_id,
            },
            fence,
        )
    }

    #[cfg(feature = "cluster")]
    fn install_single_owner_predecessor(
        operator: &mut IntervalJoinOperator,
        target: &laminar_core::checkpoint::CheckpointAssignmentFence,
    ) -> laminar_core::checkpoint::CheckpointAssignmentFence {
        let owners = vec![1; target.vnode_count as usize];
        let registry =
            VnodeRegistry::single_owner(target.vnode_count, laminar_core::state::NodeId(1));
        let predecessor_version = target.assignment_version - 1;
        if predecessor_version > registry.assignment_version() {
            registry.set_assignment_and_version(
                vec![laminar_core::state::NodeId(1); target.vnode_count as usize].into(),
                predecessor_version,
            );
        }
        operator.local_assignment = registry.versioned_snapshot();
        laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
            target.assignment_version - 1,
            &owners,
            target.participants.clone(),
        )
        .unwrap()
    }

    #[cfg(feature = "cluster")]
    async fn two_owner_shuffle() -> (
        ClusterShuffleConfig,
        Arc<laminar_core::shuffle::ShuffleReceiver>,
    ) {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::cluster::control::LeaseDeadline;
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use laminar_core::state::{NodeId, VnodeRegistry};

        let registry = Arc::new(VnodeRegistry::new(2));
        registry.set_assignment(vec![NodeId(1), NodeId(2)].into());
        let fence = CheckpointAssignmentFence::from_owner_map(
            registry.assignment_version(),
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
        let sender = Arc::new(ShuffleSender::new(1, uuid::Uuid::from_u128(1)));
        sender.register_peer(2, remote_receiver.local_addr());
        let local_deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
        local_receiver
            .install_process_lease_deadline(Arc::clone(&local_deadline))
            .unwrap();
        sender
            .install_process_lease_deadline(local_deadline)
            .unwrap();
        remote_receiver
            .install_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(
                60,
            ))))
            .unwrap();
        local_receiver
            .install_assignment_fence(&fence, &[1, 2])
            .unwrap();
        remote_receiver
            .install_assignment_fence(&fence, &[1, 2])
            .unwrap();
        sender.install_assignment_fence(&fence, &[1, 2]).unwrap();

        (
            ClusterShuffleConfig {
                registry,
                sender,
                receiver: local_receiver,
                self_id: NodeId(1),
            },
            remote_receiver,
        )
    }

    fn test_config() -> StreamJoinConfig {
        StreamJoinConfig {
            join_type: laminar_sql::parser::join_parser::JoinType::Inner,
            left_keys: vec!["id".to_string()],
            right_keys: vec!["id".to_string()],
            left_time_column: "ts".to_string(),
            right_time_column: "ts".to_string(),
            left_table: "left_stream".to_string(),
            right_table: "right_stream".to_string(),
            time_bound: Duration::from_millis(100),
        }
    }

    fn unconstrained_frontier() -> InputFrontier {
        InputFrontier {
            watermark: Some(i64::MAX),
            idle: true,
        }
    }

    #[test]
    fn output_frontier_uses_the_preserved_output_side() {
        use laminar_sql::parser::join_parser::JoinType;

        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ] {
            let mut config = test_config();
            config.join_type = join_type;
            let mut operator =
                IntervalJoinOperator::new("frontier", config, None, SessionContext::new());
            operator.applied_left_watermark = 2_000;
            operator.applied_right_watermark = 1_500;
            operator.applied_left_idle = true;
            operator.applied_right_idle = true;
            let expected = if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti) {
                1_500
            } else {
                1_400
            };
            let output = operator.output_frontier(unconstrained_frontier());
            assert_eq!(output.watermark, Some(expected), "{join_type:?}");
            assert!(output.idle, "{join_type:?}");
        }
    }

    #[tokio::test]
    async fn all_input_idle_is_checkpointed_and_restored() {
        let mut operator =
            IntervalJoinOperator::new("idle-frontier", test_config(), None, SessionContext::new());
        let frontiers = [
            InputFrontier {
                watermark: Some(200),
                idle: true,
            },
            InputFrontier {
                watermark: Some(400),
                idle: true,
            },
        ];
        operator
            .process_with_frontiers(&[Vec::new(), Vec::new()], &frontiers)
            .await
            .unwrap();

        let checkpoint = operator.checkpoint().unwrap().unwrap();
        let mut restored =
            IntervalJoinOperator::new("idle-frontier", test_config(), None, SessionContext::new());
        restored.restore(checkpoint).unwrap();

        assert!(restored.applied_left_idle);
        assert!(restored.applied_right_idle);
        let output = restored.output_frontier(InputFrontier {
            watermark: Some(i64::MAX),
            idle: false,
        });
        assert_eq!(
            output,
            InputFrontier {
                watermark: Some(200),
                idle: true,
            }
        );
        #[cfg(feature = "cluster")]
        assert_eq!(restored.restored_output_frontier(), Some(output));
    }

    fn left_batch(ids: &[&str], timestamps: &[i64], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn right_batch(ids: &[&str], timestamps: &[i64], amounts: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("amount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn key_for_vnode(target: u32, vnode_count: u32) -> String {
        for candidate in 0..1_000 {
            let key = format!("vnode-{candidate}");
            let batch = left_batch(&[key.as_str()], &[100], &[1.0]);
            let vnodes = laminar_core::shuffle::row_vnodes(&batch, &[0], vnode_count).unwrap();
            if vnodes == [target] {
                return key;
            }
        }
        panic!("could not find a key for vnode {target}");
    }

    #[cfg(feature = "cluster")]
    fn composite_left_batch(regions: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("region", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["hot"; regions.len()])),
                Arc::new(Int64Array::from(regions.to_vec())),
                Arc::new(TimestampMillisecondArray::from(vec![100; regions.len()])),
                Arc::new(Float64Array::from(vec![1.0; regions.len()])),
            ],
        )
        .unwrap()
    }

    #[cfg(feature = "cluster")]
    fn incompatible_left_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["bad"])),
                Arc::new(TimestampMillisecondArray::from(vec![110])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_basic_interval_join() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        let left = left_batch(&["A", "B"], &[100, 200], &[10.0, 20.0]);
        let right = right_batch(&["A", "B"], &[110, 250], &[1.0, 2.0]);

        let result = op
            .process(&[vec![left], vec![right]], &[0, 0])
            .await
            .unwrap();

        // A: |100 - 110| = 10 <= 100 -> match
        // B: |200 - 250| = 50 <= 100 -> match
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn local_join_routes_into_configured_vnodes() {
        let key_group_count = KeyGroupCount::try_from(8_u16).unwrap();
        let mut op = IntervalJoinOperator::new_with_key_groups(
            "local_vnodes",
            test_config(),
            None,
            laminar_sql::create_session_context(),
            key_group_count,
        );
        let key_zero = key_for_vnode(0, u32::from(key_group_count));
        let key_one = key_for_vnode(1, u32::from(key_group_count));
        let keys = [key_zero.as_str(), key_one.as_str()];

        let output = op
            .process(
                &[
                    vec![left_batch(&keys, &[100, 200], &[10.0, 20.0])],
                    vec![right_batch(&keys, &[110, 210], &[1.0, 2.0])],
                ],
                &[0, 0],
            )
            .await
            .unwrap();

        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(op.vnode_states[0].is_some());
        assert!(op.vnode_states[1].is_some());
        assert!(op.vnode_states[2..].iter().all(Option::is_none));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_admits_current_batch_before_advancing_its_watermark() {
        let (shuffle, _) = single_owner_shuffle(8).await;
        let mut op = IntervalJoinOperator::new(
            "current_batch_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);

        let output = op
            .process(
                &[
                    vec![left_batch(&["A"], &[100], &[1.0])],
                    vec![right_batch(&["A"], &[110], &[2.0])],
                ],
                &[300, 300],
            )
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        let error = op
            .process(
                &[vec![left_batch(&["late"], &[100], &[1.0])], vec![]],
                &[300, 300],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("below closed cutoff 300"));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn zero_admission_send_retries_without_becoming_runnable() {
        let (scope, _) = two_owner_shuffle().await;
        let mut operator = IntervalJoinOperator::new(
            "retry_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        operator.attach_cluster_shuffle(scope);
        let retry_plan = vec![(
            2,
            laminar_core::shuffle::ShuffleMessage::Frontier {
                stage: "retry_interval::left".to_string(),
                watermark: None,
                idle: false,
            },
        )];
        let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
        let (visible_tx, visible_rx) = tokio::sync::oneshot::channel();
        let send = tokio::spawn(async move {
            let _ = outcome_tx.send((
                Err(DbError::ShuffleNotReady("injected zero admission".into())),
                Some(retry_plan),
            ));
            let _ = visible_tx.send(());
        });
        operator.pending_cluster_input = Some(PendingIntervalClusterInput {
            routed: BTreeMap::new(),
            outbound: None,
            local_frontiers: [InputFrontier::default(); 2],
            send: Some(send),
            outcome: Some(outcome_rx),
            accounted_bytes: 0,
        });
        visible_rx.await.unwrap();

        let output = operator
            .process_cluster(
                &[Vec::new(), Vec::new()],
                InputFrontier::default(),
                InputFrontier::default(),
            )
            .await
            .unwrap();
        assert!(output.is_empty());
        assert!(operator
            .pending_cluster_input
            .as_ref()
            .unwrap()
            .send
            .is_some());
        assert!(operator
            .pending_cluster_input
            .as_ref()
            .unwrap()
            .outcome
            .is_some());
        assert!(!operator.deferred_work_is_runnable());
        assert!(operator.checkpoint_capture(u64::MAX).is_err());
        assert!(operator.checkpoint_vnodes(&[0], 2, u64::MAX).is_err());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn pending_send_applies_remote_match_before_local_finalize() {
        use laminar_sql::parser::join_parser::JoinType;

        let (scope, _) = two_owner_shuffle().await;
        let mut config = test_config();
        config.join_type = JoinType::Left;
        let mut operator = IntervalJoinOperator::new(
            "pending_interval",
            config,
            None,
            laminar_sql::create_session_context(),
        );
        operator.attach_cluster_shuffle(scope.clone());
        let local_key = key_for_vnode(0, 2);
        let remote_key = key_for_vnode(1, 2);
        let local = left_batch(&[local_key.as_str()], &[100], &[8.0]);
        let outbound = left_batch(&[remote_key.as_str()], &[100], &[1.0]);
        let close = InputFrontier {
            watermark: Some(300),
            idle: false,
        };
        let assignment = scope.registry.versioned_snapshot();
        let plan = operator
            .plan_cluster_inputs(
                &[vec![local, outbound], Vec::new()],
                [close; 2],
                &scope,
                &assignment,
                &[2],
            )
            .unwrap();
        let accounted_bytes = operator.cluster_input_plan_bytes(&plan).unwrap();
        let IntervalClusterInputPlan {
            routed,
            outbound,
            local_frontiers,
            effective_frontiers: _,
        } = plan;
        let (release, wait) = tokio::sync::oneshot::channel();
        let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
        let (visible_tx, visible_rx) = tokio::sync::oneshot::channel();
        let send = tokio::spawn(async move {
            let _ = wait.await;
            drop(outbound);
            let _ = outcome_tx.send((Ok(()), None));
            let _ = visible_tx.send(());
        });
        operator.pending_cluster_input = Some(PendingIntervalClusterInput {
            routed,
            outbound: None,
            local_frontiers,
            send: Some(send),
            outcome: Some(outcome_rx),
            accounted_bytes,
        });
        let assignment_version = scope.registry.assignment_version();
        let recovery_gen = scope.receiver.recovery_gen();
        operator
            .stage_checkpointed_shuffle(
                "pending_interval::right",
                crate::operator::RetainedBatch::restored_channel(
                    right_batch(&[local_key.as_str()], &[110], &[34.0]),
                    2,
                    assignment_version,
                    recovery_gen,
                    Arc::from([0_u32]),
                ),
                i64::MIN,
            )
            .unwrap();
        operator
            .stage_checkpointed_shuffle_frontier(
                "pending_interval::right",
                2,
                close,
                assignment_version,
                recovery_gen,
            )
            .unwrap();

        let output = tokio::time::timeout(
            Duration::from_millis(50),
            operator.process_cluster(
                &[Vec::new(), Vec::new()],
                InputFrontier::default(),
                InputFrontier::default(),
            ),
        )
        .await
        .expect("pending interval send blocked the graph task")
        .unwrap();
        assert!(output.is_empty());
        assert_eq!(operator.queued_remote_events, 1);
        assert!(operator.pending_cluster_input.is_some());

        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), visible_rx)
            .await
            .expect("pending interval send outcome was not published")
            .unwrap();
        let output = operator
            .process_cluster(
                &[Vec::new(), Vec::new()],
                InputFrontier::default(),
                InputFrontier::default(),
            )
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let amount = output[0]
            .column_by_name("amount_right_stream")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(amount.value(0), 34.0);
        assert!(operator.pending_cluster_input.is_none());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn ordered_channel_checkpoint_restores_scope_and_rejects_idle_data() {
        let (scope, _) = two_owner_shuffle().await;
        let mut operator = IntervalJoinOperator::new(
            "channel_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        operator.attach_cluster_shuffle(scope.clone());
        let key = key_for_vnode(0, 2);
        let assignment_version = scope.registry.assignment_version();
        let recovery_gen = scope.receiver.recovery_gen();
        let close = InputFrontier {
            watermark: Some(300),
            idle: false,
        };
        operator
            .stage_checkpointed_shuffle(
                "channel_interval::right",
                crate::operator::RetainedBatch::restored_channel(
                    right_batch(&[key.as_str()], &[110], &[2.0]),
                    2,
                    assignment_version,
                    recovery_gen,
                    Arc::from([0_u32]),
                ),
                i64::MIN,
            )
            .unwrap();
        operator
            .stage_checkpointed_shuffle_frontier(
                "channel_interval::right",
                2,
                close,
                assignment_version,
                recovery_gen,
            )
            .unwrap();
        operator
            .stage_checkpointed_shuffle_frontier(
                "channel_interval::left",
                2,
                close,
                assignment_version,
                recovery_gen,
            )
            .unwrap();
        let IntervalRemoteEventPayload::Data(queued) = &operator.peer_channels
            [JoinInputSide::Right.port()][&2]
            .events
            .front()
            .unwrap()
            .payload
        else {
            panic!("right channel did not retain its staged data first");
        };
        assert_eq!(queued.row_vnodes.as_ref(), &[0]);
        operator.remote_side_cursor = 1;
        let checkpoint = operator.checkpoint().unwrap().unwrap();
        let checkpoint_data = checkpoint.data.clone();
        let active_recovery = recovery_gen + 1;
        scope.sender.set_recovery_gen(active_recovery);
        scope.receiver.set_recovery_gen(active_recovery);

        let mut malformed =
            rkyv::from_bytes::<IntervalJoinOperatorCheckpoint, rkyv::rancor::Error>(
                &checkpoint_data,
            )
            .unwrap();
        malformed.applied_right_watermark = 200;
        let right_channel =
            &mut malformed.cluster.as_mut().unwrap().channels[JoinInputSide::Right.port()][0];
        right_channel.applied = IntervalCheckpointFrontier {
            watermark: Some(100),
            idle: true,
        };
        right_channel.events.insert(
            0,
            IntervalCheckpointEvent::Frontier {
                recovery_gen,
                frontier: IntervalCheckpointFrontier {
                    watermark: Some(150),
                    idle: false,
                },
            },
        );
        let mut malformed_target = IntervalJoinOperator::new(
            "channel_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        malformed_target.attach_cluster_shuffle(scope.clone());
        let error = malformed_target
            .restore(OperatorCheckpoint {
                data: rkyv::to_bytes::<rkyv::rancor::Error>(&malformed)
                    .unwrap()
                    .to_vec(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("revival frontier is below"));

        let mut rejected = IntervalJoinOperator::new(
            "channel_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        rejected.attach_cluster_shuffle(scope.clone());
        let prior_key = "p".repeat(checkpoint_data.len().saturating_mul(2).max(1));
        let mut prior_state = IntervalJoinState::new();
        execute_interval_join_cycle(
            &mut prior_state,
            &[left_batch(&[prior_key.as_str()], &[90], &[1.0])],
            &[],
            &rejected.config,
            i64::MIN,
            i64::MIN,
            i64::MIN,
            i64::MIN,
            usize::MAX,
            &mut IntervalJoinOutputBudget::default(),
        )
        .unwrap();
        rejected.vnode_states[0] = Some(Box::new(prior_state));
        rejected.add_resident_vnode(0);
        let prior_channel_frontiers = [
            InputFrontier {
                watermark: Some(17),
                idle: false,
            },
            InputFrontier {
                watermark: Some(19),
                idle: false,
            },
        ];
        for (port, frontier) in prior_channel_frontiers.into_iter().enumerate() {
            let channel = rejected.peer_channels[port].get_mut(&2).unwrap();
            channel.applied = frontier;
            channel.accepted = frontier;
        }
        rejected.remote_side_cursor = 0;
        rejected.remote_peer_cursors = [Some(2), Some(2)];
        let prior_state_ptr = std::ptr::from_ref(rejected.vnode_states[0].as_deref().unwrap());
        let baseline = rejected.accounted_state_bytes();
        assert!(checkpoint_data.len() <= baseline);
        rejected.set_managed_state_budget(baseline);
        let error = rejected
            .restore(OperatorCheckpoint {
                data: checkpoint_data.clone(),
            })
            .unwrap_err();
        let DbError::ManagedStateBudgetExceeded {
            context,
            accounted_bytes,
            limit_bytes,
        } = error
        else {
            panic!("cluster restore did not reject its projected state budget");
        };
        assert!(context.contains("cluster checkpoint restore"));
        assert_eq!(limit_bytes, baseline);
        assert!(accounted_bytes > baseline);
        assert_eq!(
            std::ptr::from_ref(rejected.vnode_states[0].as_deref().unwrap()),
            prior_state_ptr
        );
        assert_eq!(
            rejected.vnode_states[0].as_deref().unwrap().buffered_rows(),
            (1, 0)
        );
        assert_eq!(rejected.resident_vnodes, [0]);
        assert_eq!(rejected.applied_left_watermark, i64::MIN);
        assert_eq!(rejected.applied_right_watermark, i64::MIN);
        assert!(!rejected.applied_left_idle);
        assert!(!rejected.applied_right_idle);
        assert_eq!(rejected.local_frontiers, [InputFrontier::default(); 2]);
        assert_eq!(rejected.last_broadcasts, [InputFrontier::default(); 2]);
        assert_eq!(rejected.remote_side_cursor, 0);
        assert_eq!(rejected.remote_peer_cursors, [Some(2), Some(2)]);
        assert_eq!(rejected.queued_remote_events, 0);
        assert_eq!(rejected.queued_shuffle_bytes, 0);
        assert_eq!(rejected.queued_event_capacity_bytes, 0);
        assert!(rejected.pending_cluster_input.is_none());
        for (port, frontier) in prior_channel_frontiers.into_iter().enumerate() {
            let channel = &rejected.peer_channels[port][&2];
            assert_eq!(channel.applied, frontier);
            assert_eq!(channel.accepted, frontier);
            assert!(channel.events.is_empty());
        }

        let mut restored = IntervalJoinOperator::new(
            "channel_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        restored.attach_cluster_shuffle(scope);
        restored.restore(checkpoint).unwrap();
        assert_eq!(restored.queued_remote_events, 3);
        let IntervalRemoteEventPayload::Data(queued) = &restored.peer_channels
            [JoinInputSide::Right.port()][&2]
            .events
            .front()
            .unwrap()
            .payload
        else {
            panic!("restored right channel lost its staged data order");
        };
        assert_eq!(queued.row_vnodes.as_ref(), &[0]);
        assert!(!restored.wants_input());
        assert!(restored
            .checkpoint_vnodes(&[0], 2, u64::MAX)
            .unwrap()
            .is_some());

        let first = restored
            .process_cluster(
                &[Vec::new(), Vec::new()],
                InputFrontier::default(),
                InputFrontier::default(),
            )
            .await
            .unwrap();
        assert!(first.is_empty());
        assert_eq!(restored.queued_remote_events, 2);
        assert_eq!(
            restored.vnode_states[0].as_ref().unwrap().buffered_rows(),
            (0, 1)
        );
        assert_eq!(
            restored.peer_channels[JoinInputSide::Right.port()][&2].applied,
            InputFrontier::default()
        );
        let second = restored
            .process_cluster(
                &[Vec::new(), Vec::new()],
                InputFrontier::default(),
                InputFrontier::default(),
            )
            .await
            .unwrap();
        assert!(second.is_empty());
        assert_eq!(restored.queued_remote_events, 1);
        assert_eq!(
            restored.peer_channels[JoinInputSide::Left.port()][&2].applied,
            close
        );
        assert_eq!(
            restored.peer_channels[JoinInputSide::Right.port()][&2].applied,
            InputFrontier::default()
        );
        assert_eq!(
            restored.vnode_states[0].as_ref().unwrap().buffered_rows(),
            (0, 1)
        );
        let third = restored
            .process_cluster(
                &[Vec::new(), Vec::new()],
                InputFrontier::default(),
                InputFrontier::default(),
            )
            .await
            .unwrap();
        assert!(third.is_empty());
        assert_eq!(restored.queued_remote_events, 0);
        assert_eq!(
            restored.peer_channels[JoinInputSide::Right.port()][&2].applied,
            close
        );
        assert_eq!(
            restored.vnode_states[0].as_ref().unwrap().buffered_rows(),
            (0, 1)
        );

        restored
            .stage_checkpointed_shuffle_frontier(
                "channel_interval::left",
                2,
                InputFrontier {
                    watermark: close.watermark,
                    idle: true,
                },
                assignment_version,
                active_recovery,
            )
            .unwrap();
        let error = restored
            .stage_checkpointed_shuffle(
                "channel_interval::left",
                crate::operator::RetainedBatch::restored_channel(
                    left_batch(&[key.as_str()], &[100], &[1.0]),
                    2,
                    assignment_version,
                    active_recovery,
                    Arc::from([0_u32]),
                ),
                i64::MIN,
            )
            .unwrap_err();
        assert!(error.to_string().contains("behind an idle peer frontier"));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn pending_plan_budget_rejects_before_shuffle_admission() {
        let (shuffle, remote_receiver) = two_owner_shuffle().await;
        let local_key = key_for_vnode(0, 2);
        let remote_key = key_for_vnode(1, 2);
        let mut op = IntervalJoinOperator::new(
            "post_shuffle_failure",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);
        let baseline = op.accounted_state_bytes();
        op.max_managed_state_bytes = baseline;

        let error = op
            .process(
                &[
                    vec![left_batch(
                        &[local_key.as_str(), remote_key.as_str()],
                        &[100, 100],
                        &[1.0, 2.0],
                    )],
                    vec![],
                ],
                &[0, 0],
            )
            .await
            .unwrap_err();

        let DbError::ManagedStateBudgetExceeded {
            context,
            accounted_bytes,
            limit_bytes,
        } = error
        else {
            panic!("pending interval plan did not fail its managed-state budget");
        };
        assert!(context.contains("pending shuffle send"));
        assert_eq!(limit_bytes, baseline);
        assert!(accounted_bytes > baseline);
        assert!(op.pending_cluster_input.is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), remote_receiver.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_cross_cycle_matching() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        // Cycle 1: only left data
        let left = left_batch(&["A"], &[100], &[10.0]);
        let result = op.process(&[vec![left], vec![]], &[0, 0]).await.unwrap();
        assert!(result.is_empty());

        // Cycle 2: right data arrives, should match the buffered left
        let right = right_batch(&["A"], &[150], &[1.0]);
        let result = op.process(&[vec![], vec![right]], &[0, 0]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_empty_inputs() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        let result = op.process(&[], &[0]).await.unwrap();
        assert!(result.is_empty());
        assert!(op.vnode_states[0].is_none());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn composite_keys_route_by_the_full_ordered_tuple() {
        let vnode_count = 64;
        let batch = composite_left_batch(&[1, 2]);
        let mut expected = laminar_core::shuffle::row_vnodes(&batch, &[0, 1], vnode_count).unwrap();
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(expected.len(), 2, "test tuple hashes unexpectedly collided");

        let (shuffle, _) = single_owner_shuffle(vnode_count).await;
        let mut config = test_config();
        config.left_keys.push("region".into());
        config.right_keys.push("region".into());
        let mut op = IntervalJoinOperator::new(
            "composite_interval",
            config,
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);
        op.process(&[vec![batch], vec![]], &[0, 0]).await.unwrap();

        let actual = op
            .vnode_states
            .iter()
            .zip(0_u32..)
            .filter_map(|(state, vnode)| state.as_ref().map(|_| vnode))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn later_vnode_failure_requires_recovery_after_prior_admission() {
        let (shuffle, _) = single_owner_shuffle(2).await;
        let mut op = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);

        let mut retained = IntervalJoinState::new();
        let mut output_budget = IntervalJoinOutputBudget::default();
        execute_interval_join_cycle(
            &mut retained,
            &[left_batch(&["seed"], &[100], &[1.0])],
            &[],
            &op.config,
            0,
            0,
            0,
            0,
            usize::MAX,
            &mut output_budget,
        )
        .unwrap();
        op.vnode_states[1] = Some(Box::new(retained));
        op.add_resident_vnode(1);

        let mut routed = BTreeMap::new();
        routed.insert(0, [vec![left_batch(&["ok"], &[100], &[1.0])], vec![]]);
        routed.insert(1, [vec![incompatible_left_batch()], vec![]]);
        let error = op.execute_routed_shards(routed, 0, 0).unwrap_err();

        assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
        assert_eq!(op.vnode_states[0].as_ref().unwrap().buffered_rows(), (1, 0));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_watermark_sweep_failure_requires_recovery() {
        let (shuffle, _) = single_owner_shuffle(2).await;
        let mut op = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);

        let mut retained = IntervalJoinState::new();
        let mut output_budget = IntervalJoinOutputBudget::default();
        for timestamp in (1_000..1_400).step_by(10) {
            execute_interval_join_cycle(
                &mut retained,
                &[left_batch(&["seed"], &[timestamp], &[1.0])],
                &[],
                &op.config,
                0,
                0,
                0,
                0,
                usize::MAX,
                &mut output_budget,
            )
            .unwrap();
        }
        op.vnode_states[0] = Some(Box::new(retained));
        op.add_resident_vnode(0);

        // Force compaction to fail after the sweep has already removed old index entries.
        op.config.left_keys = vec!["missing".to_string()];
        let error = op.process(&[], &[0, 1_300]).await.unwrap_err();

        assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
        assert!(error.requires_pipeline_recovery());
        assert_eq!(
            op.vnode_states[0].as_ref().unwrap().buffered_rows(),
            (20, 0)
        );
    }

    #[tokio::test]
    async fn test_checkpoint_roundtrip() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx.clone());

        // Buffer some data
        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A"], &[110], &[1.0]);
        let _ = op
            .process(&[vec![left], vec![right]], &[50, 50])
            .await
            .unwrap();
        assert_eq!(
            op.output_frontier(unconstrained_frontier()).watermark,
            Some(-50)
        );

        let metadata = op
            .checkpoint()
            .unwrap()
            .expect("watermarks are checkpointed");
        let captured = op
            .checkpoint_vnodes(&[0], 1, u64::MAX)
            .unwrap()
            .expect("interval join has vnode state");
        let state = captured
            .into_iter()
            .next()
            .and_then(|captured| captured.state)
            .expect("the first vnode capture is complete");
        assert!(op
            .checkpoint_vnodes(&[0], 1, u64::MAX)
            .unwrap()
            .unwrap()
            .is_empty());
        let state = materialize_capture(state).unwrap();

        let mut op2 = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);
        op2.restore(metadata).unwrap();
        op2.restore_vnode(0, 1, &state).unwrap();
        assert_eq!(
            op2.output_frontier(unconstrained_frontier()).watermark,
            Some(-50)
        );

        // New right data should match the restored left
        let right2 = right_batch(&["A"], &[120], &[2.0]);
        let result = op2
            .process(&[vec![], vec![right2]], &[50, 50])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn vnode_capture_is_full_then_sparse_until_forced() {
        let vnode_count = 4_u32;
        let required = [0, 1, 2, 3];
        let mut op = IntervalJoinOperator::new_with_key_groups(
            "sparse_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
            KeyGroupCount::try_from(4_u16).unwrap(),
        );

        let baseline = op
            .checkpoint_vnodes(&required, vnode_count, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(
            baseline.iter().map(|frame| frame.vnode).collect::<Vec<_>>(),
            required
        );
        assert!(baseline.iter().all(|frame| frame.state.is_some()));
        drop(baseline);

        assert!(op
            .checkpoint_vnodes(&required, vnode_count, u64::MAX)
            .unwrap()
            .unwrap()
            .is_empty());

        let vnode = 2;
        let key = key_for_vnode(vnode, vnode_count);
        op.process(
            &[vec![left_batch(&[key.as_str()], &[100], &[1.0])], vec![]],
            &[0, 0],
        )
        .await
        .unwrap();
        let dirty = op
            .checkpoint_vnodes(&required, vnode_count, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].vnode, vnode);
        assert!(dirty[0].state.is_some());
        drop(dirty);

        op.force_full_vnode_capture();
        let forced = op
            .checkpoint_vnodes(&required, vnode_count, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(
            forced.iter().map(|frame| frame.vnode).collect::<Vec<_>>(),
            required
        );
        assert!(forced.iter().all(|frame| frame.state.is_some()));
    }

    #[tokio::test]
    async fn checkpoint_respects_ipc_and_archive_peak_budget() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);
        let wide_key = "x".repeat(64 * 1024);
        op.process(
            &[
                vec![left_batch(&[wide_key.as_str()], &[100], &[1.0])],
                vec![],
            ],
            &[0, 0],
        )
        .await
        .unwrap();

        let ipc_bytes = op.vnode_states[0]
            .as_mut()
            .unwrap()
            .snapshot_checkpoint(&op.config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap()
            .retained_ipc_bytes()
            .unwrap();
        let archive_bytes = IntervalJoinOperator::serialize_state(
            op.vnode_states[0].as_mut().unwrap(),
            &op.config,
            "interval join peak-budget sizing",
            usize::MAX,
        )
        .unwrap()
        .len();
        let limit = ipc_bytes
            .checked_add(archive_bytes)
            .and_then(|bytes| bytes.checked_add(HEAP_ALLOCATION_CHARGE))
            .unwrap();
        assert!(op.accounted_state_bytes() <= limit);
        op.set_managed_state_budget(limit);

        let state = op
            .checkpoint_vnodes(&[0], 1, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .next()
            .and_then(|captured| captured.state)
            .expect("the first vnode capture is complete");
        assert!(matches!(&state, StateFrameCapture::Deferred { .. }));
        let state = materialize_capture(state).unwrap();
        assert_eq!(state.len(), archive_bytes);
    }

    #[test]
    fn deferred_vnode_frames_share_the_operator_checkpoint_budget() {
        let make_operator = || {
            let mut operator = IntervalJoinOperator::new_with_key_groups(
                "test_interval",
                test_config(),
                None,
                laminar_sql::create_session_context(),
                KeyGroupCount::try_from(2_u16).unwrap(),
            );
            for state in &mut operator.vnode_states {
                *state = Some(Box::new(IntervalJoinState::new()));
            }
            operator
        };

        let mut sizing = make_operator();
        let capture = sizing
            .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .next()
            .and_then(|captured| captured.state)
            .unwrap();
        let retained_bytes = sizing.vnode_states[0]
            .as_mut()
            .unwrap()
            .snapshot_checkpoint(&sizing.config, usize::MAX)
            .unwrap()
            .retained_ipc_bytes()
            .unwrap();
        let frame_bytes = materialize_capture(capture).unwrap().len();
        let single_frame_peak = retained_bytes
            .checked_add(HEAP_ALLOCATION_CHARGE)
            .and_then(|bytes| bytes.checked_add(frame_bytes))
            .unwrap();
        let limit = single_frame_peak.checked_add(frame_bytes).unwrap() - 1;

        let mut peak_operator = make_operator();
        peak_operator.vnode_states[1] = None;
        peak_operator.set_managed_state_budget(single_frame_peak - 1);
        let peak_capture = peak_operator
            .checkpoint_vnodes(&[0], 2, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .next()
            .and_then(|captured| captured.state)
            .unwrap();
        assert!(materialize_capture(peak_capture).is_err());

        let mut operator = make_operator();
        operator.set_managed_state_budget(limit);
        let mut frames = operator
            .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|captured| captured.state.unwrap());
        materialize_capture(frames.next().unwrap()).unwrap();
        assert!(materialize_capture(frames.next().unwrap()).is_err());
    }

    #[tokio::test]
    async fn vnode_restore_preserves_sparse_state() {
        let key_group_count = KeyGroupCount::try_from(2_u16).unwrap();
        let key = key_for_vnode(1, 2);
        let mut donor = IntervalJoinOperator::new_with_key_groups(
            "sparse_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
            key_group_count,
        );
        donor
            .process(
                &[vec![left_batch(&[key.as_str()], &[100], &[1.0])], vec![]],
                &[0, 0],
            )
            .await
            .unwrap();
        let frames = donor
            .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|frame| {
                (
                    frame.vnode,
                    materialize_capture(frame.state.unwrap()).unwrap(),
                )
            })
            .collect::<Vec<_>>();

        let mut restored = IntervalJoinOperator::new_with_key_groups(
            "sparse_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
            key_group_count,
        );
        for (vnode, state) in frames {
            restored.restore_vnode(vnode, 2, &state).unwrap();
        }

        assert!(restored.vnode_states[0].is_none() && restored.vnode_states[1].is_some());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn fresh_owner_stages_common_portable_cut_atomically() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::state::NodeId;

        use crate::operator_graph::{ManagedVnodeRestore, ManagedWholeRestore};

        let vnode_count = 2;
        let (scope, target_fence) = single_owner_shuffle(vnode_count).await;
        let mut target = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        target.attach_cluster_shuffle(scope.clone());
        let predecessor_version = target_fence.assignment_version - 1;
        let predecessor_owners = [2_u64, 3];
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            predecessor_version,
            &predecessor_owners,
            vec![
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
        let predecessor_registry = VnodeRegistry::new_unassigned(vnode_count);
        predecessor_registry.set_assignment_and_version(
            Arc::from(predecessor_owners.map(NodeId)),
            predecessor_version,
        );
        target.local_assignment = predecessor_registry.versioned_snapshot();

        let mut empty = IntervalJoinState::new();
        let vnode0 = IntervalJoinOperator::serialize_state(
            &mut empty,
            &target.config,
            "test vnode 0",
            target.max_managed_state_bytes,
        )
        .unwrap();
        let vnode1 = IntervalJoinOperator::serialize_state(
            &mut IntervalJoinState::new(),
            &target.config,
            "test vnode 1",
            target.max_managed_state_bytes,
        )
        .unwrap();
        let restores = [
            ManagedVnodeRestore {
                participant_id: 2,
                vnode: 0,
                state: &vnode0,
            },
            ManagedVnodeRestore {
                participant_id: 3,
                vnode: 1,
                state: &vnode1,
            },
        ];
        let donor_config = target.config.clone();
        let encode_whole = |participant_id: u64, right_watermark: i64, left_idle: bool| {
            let checkpoint = IntervalJoinOperatorCheckpoint {
                version: OPERATOR_CHECKPOINT_VERSION,
                join_type: join_type_tag(donor_config.join_type),
                left_keys: donor_config.left_keys.clone(),
                right_keys: donor_config.right_keys.clone(),
                left_time_column: donor_config.left_time_column.clone(),
                right_time_column: donor_config.right_time_column.clone(),
                left_table: donor_config.left_table.clone(),
                right_table: donor_config.right_table.clone(),
                bound_ms: i64::try_from(donor_config.time_bound.as_millis()).unwrap(),
                applied_left_watermark: 300,
                applied_right_watermark: right_watermark,
                applied_left_idle: left_idle,
                applied_right_idle: false,
                cluster: Some(IntervalClusterCheckpoint {
                    assignment_version: predecessor.assignment_version,
                    owner_map_digest: predecessor.assignment_digest,
                    self_id: participant_id,
                    recovery_gen: 1,
                    local_frontiers: [
                        IntervalCheckpointFrontier {
                            watermark: Some(300),
                            idle: left_idle,
                        },
                        IntervalCheckpointFrontier {
                            watermark: Some(right_watermark),
                            idle: false,
                        },
                    ],
                    remote_side_cursor: 0,
                    remote_peer_cursors: [None; 2],
                    channels: [Vec::new(), Vec::new()],
                }),
            };
            rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
                .unwrap()
                .to_vec()
        };
        let donor2 = encode_whole(2, 250, true);
        let disagreeing_donor3 = encode_whole(3, 251, false);
        let disagreeing = [
            ManagedWholeRestore {
                participant_id: 2,
                state: &donor2,
            },
            ManagedWholeRestore {
                participant_id: 3,
                state: &disagreeing_donor3,
            },
        ];
        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &disagreeing,
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap_err();
        assert_eq!(target.applied_left_watermark, i64::MIN);
        assert!(target.vnode_states.iter().all(Option::is_none));

        let donor3 = encode_whole(3, 250, true);
        let whole_restores = [
            ManagedWholeRestore {
                participant_id: 2,
                state: &donor2,
            },
            ManagedWholeRestore {
                participant_id: 3,
                state: &donor3,
            },
        ];

        let mut stale = IntervalJoinState::new();
        let stale_key = key_for_vnode(0, vnode_count);
        execute_interval_join_cycle(
            &mut stale,
            &[left_batch(&[stale_key.as_str()], &[200], &[1.0])],
            &[],
            &target.config,
            i64::MIN,
            i64::MIN,
            i64::MIN,
            i64::MIN,
            target.max_managed_state_bytes,
            &mut IntervalJoinOutputBudget::default(),
        )
        .unwrap();
        let stale_vnode0 = IntervalJoinOperator::serialize_state(
            &mut stale,
            &target.config,
            "stale vnode 0",
            target.max_managed_state_bytes,
        )
        .unwrap();
        let stale_restores = [
            ManagedVnodeRestore {
                participant_id: 2,
                vnode: 0,
                state: &stale_vnode0,
            },
            ManagedVnodeRestore {
                participant_id: 3,
                vnode: 1,
                state: &vnode1,
            },
        ];
        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &stale_restores,
                whole_restores: &whole_restores,
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap_err();
        assert!(target.vnode_states.iter().all(Option::is_none));

        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &whole_restores,
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        assert_eq!(target.applied_left_watermark, i64::MIN);
        target.publish_vnode_transition();
        assert_eq!(target.applied_left_watermark, 300);
        assert_eq!(target.applied_right_watermark, 250);
        assert!(target.applied_left_idle);
        assert!(!target.applied_right_idle);
        target.finish_vnode_transition();

        let predecessor_nodes = predecessor_owners.map(NodeId);
        let mut bootstrap = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        bootstrap.attach_cluster_shuffle(scope);
        bootstrap
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &whole_restores,
                mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
                    predecessor_owners: &predecessor_nodes,
                },
            })
            .unwrap();
        bootstrap.publish_vnode_transition();
        assert_eq!(bootstrap.applied_left_watermark, 300);
        assert_eq!(bootstrap.applied_right_watermark, 250);
        assert!(bootstrap.vnode_states.iter().all(Option::is_some));
        bootstrap.finish_vnode_transition();
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn zero_owner_topology_transition_is_not_an_acquisition() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::state::{NodeId, VnodeRegistry};

        let (scope, current) = single_owner_shuffle(1).await;
        let predecessor_version = current.assignment_version;
        let target_version = predecessor_version + 1;
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            predecessor_version,
            &[2],
            vec![CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            }],
        )
        .unwrap();
        let target = CheckpointAssignmentFence::from_owner_map(
            target_version,
            &[2],
            vec![CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(22),
            }],
        )
        .unwrap();
        scope
            .registry
            .set_assignment_and_version(Arc::from([NodeId(2)]), target_version);
        scope.sender.invalidate_assignment_fence();
        scope.receiver.invalidate_assignment_fence();

        let mut operator = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        operator.attach_cluster_shuffle(scope);
        let predecessor_registry = VnodeRegistry::new_unassigned(1);
        predecessor_registry
            .set_assignment_and_version(Arc::from([NodeId(2)]), predecessor_version);
        operator.local_assignment = predecessor_registry.versioned_snapshot();
        operator.applied_left_watermark = 100;
        operator.applied_right_watermark = 200;
        operator.local_frontiers = operator.applied_frontiers();
        operator.last_broadcasts = operator.local_frontiers;
        for port in 0..2 {
            let stale = InputFrontier {
                watermark: Some(900 + i64::try_from(port).unwrap()),
                idle: false,
            };
            let channel = operator.peer_channels[port].get_mut(&2).unwrap();
            channel.applied = stale;
            channel.accepted = stale;
        }

        operator
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &[],
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        let prepared = operator.prepared_vnode_transition.as_ref().unwrap();
        assert!(prepared.bootstrap_broadcast);
        for port in 0..2 {
            let channel = &prepared.peer_channels[port][&2];
            assert_eq!(channel.applied, operator.local_frontiers[port]);
            assert_eq!(channel.accepted, operator.local_frontiers[port]);
            assert!(channel.events.is_empty());
        }
        operator.publish_vnode_transition();

        assert_eq!(operator.local_assignment.version(), target_version);
        assert_eq!(operator.local_assignment.owners(), &[NodeId(2)]);
        assert_eq!(operator.last_broadcasts, [InputFrontier::default(); 2]);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn vnode_transition_preserves_checkpointed_admission_watermarks() {
        let vnode_count = 8;
        let (source_shuffle, _) = single_owner_shuffle(vnode_count).await;
        let mut source = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        source.attach_cluster_shuffle(source_shuffle);
        assert!(source
            .process(&[vec![], vec![]], &[300, 300])
            .await
            .unwrap()
            .is_empty());
        let checkpoint = source.checkpoint().unwrap().unwrap();

        let (restored_shuffle, fence) = single_owner_shuffle(vnode_count).await;
        let mut restored = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        restored.attach_cluster_shuffle(restored_shuffle);
        restored.restore(checkpoint).unwrap();
        let predecessor = install_single_owner_predecessor(&mut restored, &fence);
        restored
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &[],
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        restored.publish_vnode_transition();
        assert_eq!(
            restored.local_assignment.version(),
            fence.assignment_version
        );
        restored.finish_vnode_transition();

        let error = restored
            .process(
                &[vec![left_batch(&["late"], &[100], &[1.0])], vec![]],
                &[300, 300],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("below closed cutoff 300"));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn vnode_capture_restore_preserves_cross_cycle_match() {
        let vnode_count = 8;
        let key_batch = left_batch(&["hot"], &[100], &[10.0]);
        let vnode = laminar_core::shuffle::row_vnodes(&key_batch, &[0], vnode_count).unwrap()[0];

        let (donor_shuffle, _) = single_owner_shuffle(vnode_count).await;
        let mut donor = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        donor.attach_cluster_shuffle(donor_shuffle);
        assert!(donor
            .process(&[vec![key_batch], vec![]], &[0, 0])
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            donor
                .vnode_states
                .iter()
                .zip(0_u32..)
                .filter_map(|(state, vnode)| state.as_ref().map(|_| vnode))
                .collect::<Vec<_>>(),
            vec![vnode]
        );

        let captured = donor
            .checkpoint_vnodes(&[vnode], vnode_count, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(captured[0].vnode, vnode);
        let capture = captured
            .into_iter()
            .next()
            .and_then(|captured| captured.state)
            .expect("the first vnode capture is complete");
        let state = materialize_capture(capture).unwrap();

        let (restored_shuffle, fence) = single_owner_shuffle(vnode_count).await;
        let mut restored = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        restored.attach_cluster_shuffle(restored_shuffle);
        let restores = [crate::operator_graph::ManagedVnodeRestore {
            participant_id: 1,
            vnode,
            state: &state,
        }];
        let predecessor = install_single_owner_predecessor(&mut restored, &fence);
        restored
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        let prepared = restored.managed_state_accounting().unwrap();
        assert!(prepared.prepared > 0);
        assert_eq!(prepared.retired, 0);
        restored.abort_vnode_transition();
        let aborted = restored.managed_state_accounting().unwrap();
        assert!(aborted.prepared > 0);
        assert_eq!(aborted.retired, 0);
        assert_eq!(
            restored.local_assignment.version(),
            predecessor.assignment_version
        );
        restored.finish_vnode_transition();
        restored
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        restored.publish_vnode_transition();
        assert_eq!(
            restored.local_assignment.version(),
            fence.assignment_version
        );
        assert_eq!(restored.applied_left_watermark, 0);
        assert_eq!(restored.applied_right_watermark, 0);
        restored.finish_vnode_transition();

        let output = restored
            .process(
                &[vec![], vec![right_batch(&["hot"], &[110], &[1.0])]],
                &[0, 0],
            )
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(
            restored
                .vnode_states
                .iter()
                .zip(0_u32..)
                .filter_map(|(state, vnode)| state.as_ref().map(|_| vnode))
                .collect::<Vec<_>>(),
            vec![vnode]
        );

        assert!(restored
            .process(&[vec![], vec![]], &[500, 500])
            .await
            .unwrap()
            .is_empty());
        let state = restored.vnode_states[vnode as usize].as_ref().unwrap();
        assert_eq!(state.buffered_rows(), (0, 0));
    }

    #[tokio::test]
    async fn post_projection_fault_requires_recovery_after_state_admission() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            Some(Arc::from("SELECT missing FROM __interval_tmp")),
            ctx,
        );

        let error = op
            .process(
                &[
                    vec![left_batch(&["A"], &[100], &[10.0])],
                    vec![right_batch(&["A"], &[110], &[1.0])],
                ],
                &[0, 0],
            )
            .await
            .unwrap_err();

        assert!(matches!(&error, DbError::StatefulOperatorPartialApply(_)));
        assert!(error.requires_pipeline_recovery());
        let capture = op
            .checkpoint_vnodes(&[0], 1, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .next()
            .and_then(|captured| captured.state)
            .expect("state admitted before projection failure remains checkpointable");
        let state = materialize_capture(capture).unwrap();
        assert_eq!(state.first(), Some(&PRESENT_VNODE));
        assert_eq!(state.get(1), Some(&VNODE_FRAME_VERSION));
        let decoded = rkyv::from_bytes::<JoinStateCheckpoint, rkyv::rancor::Error>(
            &state[VNODE_FRAME_HEADER_LEN..],
        )
        .unwrap();
        assert_eq!(decoded.left_buffer_rows, 1);
        assert_eq!(decoded.right_buffer_rows, 1);
    }

    #[test]
    fn current_vnode_frame_version_rejects_legacy_present_and_absent_frames() {
        let config = test_config();
        let mut state = IntervalJoinState::new();
        let encoded = IntervalJoinOperator::serialize_state(
            &mut state,
            &config,
            "versioned vnode",
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        )
        .unwrap();
        assert_eq!(encoded[0], PRESENT_VNODE);
        assert_eq!(encoded[1], VNODE_FRAME_VERSION);
        assert!(IntervalJoinOperator::decode_vnode_frame(
            &ABSENT_VNODE_FRAME,
            &config,
            "current absent",
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            None,
        )
        .unwrap()
        .is_none());

        let mut legacy_present = encoded.clone();
        legacy_present[1] = 0;
        let error = IntervalJoinOperator::decode_vnode_frame(
            &legacy_present,
            &config,
            "legacy present",
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            None,
        )
        .err()
        .expect("legacy present vnode frame must fail");
        assert!(error.to_string().contains("version 0 is unsupported"));

        let legacy_absent = vec![0_u8; VNODE_FRAME_HEADER_LEN];
        let error = IntervalJoinOperator::decode_vnode_frame(
            &legacy_absent,
            &config,
            "legacy absent",
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            None,
        )
        .err()
        .expect("legacy absent vnode frame must fail");
        assert!(error.to_string().contains("version 0 is unsupported"));

        let mut malformed = ABSENT_VNODE_FRAME;
        malformed[2] = 1;
        let error = IntervalJoinOperator::decode_vnode_frame(
            &malformed,
            &config,
            "malformed current",
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            None,
        )
        .err()
        .expect("malformed current vnode frame must fail");
        assert!(error.to_string().contains("header is malformed"));
    }

    #[test]
    fn whole_checkpoint_rejects_previous_operator_version() {
        let config = test_config();
        let mut source =
            IntervalJoinOperator::new("stale-whole", config.clone(), None, SessionContext::new());
        source.applied_left_watermark = 0;
        let mut checkpoint = source
            .capture_operator_checkpoint(u64::MAX)
            .unwrap()
            .unwrap()
            .checkpoint;
        checkpoint.version = OPERATOR_CHECKPOINT_VERSION - 1;
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
            .unwrap()
            .to_vec();
        let mut target =
            IntervalJoinOperator::new("stale-whole", config, None, SessionContext::new());
        let error = target.restore(OperatorCheckpoint { data }).unwrap_err();
        assert!(error
            .to_string()
            .contains("version or configuration does not match"));
    }

    #[test]
    fn test_name() {
        let ctx = laminar_sql::create_session_context();
        let op = IntervalJoinOperator::new("my_interval_join", test_config(), None, ctx);
        assert_eq!(&*op.projection.op_name, "my_interval_join");
    }
}
