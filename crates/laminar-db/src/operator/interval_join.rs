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
    build_output_schema, build_weighted_output_schema, execute_interval_join_cycle,
    execute_weighted_interval_join_cycle, join_type_tag, IntervalJoinCheckpointCapture,
    IntervalJoinOutputBudget, IntervalJoinState, JoinStateCheckpoint, HEAP_ALLOCATION_CHARGE,
};
#[cfg(feature = "cluster")]
use crate::operator::interval_join_input::preflight_queued_batch_ipc_restore;
use crate::operator::interval_join_input::{
    normalizer_config_fingerprint, BoundedJoinInputCheckpoint, BoundedJoinInputCheckpointCapture,
    BoundedJoinInputConfig, BoundedJoinInputMode, BoundedJoinInputNormalizer,
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

const OPERATOR_CHECKPOINT_VERSION: u8 = 4;
const ABSENT_VNODE: u8 = 0;
const PRESENT_VNODE: u8 = 1;
const VNODE_FRAME_VERSION: u8 = 2;
const VNODE_FRAME_HEADER_LEN: usize = std::mem::align_of::<ArchivedIntervalVnodeCheckpoint>();
const CHECKPOINT_ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;
#[cfg(feature = "cluster")]
const WHOLE_RESTORE_ROW_SCRATCH_CHARGE: usize = 2_048;
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

fn checkpoint_alignment_copy_bytes(bytes: &[u8]) -> usize {
    if bytes.is_empty() || bytes.as_ptr().align_offset(CHECKPOINT_ARCHIVE_ALIGNMENT) == 0 {
        0
    } else {
        bytes.len().saturating_add(HEAP_ALLOCATION_CHARGE)
    }
}

fn vnode_checkpoint_alignment_copy_bytes(bytes: &[u8]) -> usize {
    bytes
        .get(VNODE_FRAME_HEADER_LEN..)
        .map_or(0, checkpoint_alignment_copy_bytes)
}

fn with_aligned_checkpoint_bytes<T>(
    bytes: &[u8],
    decode: impl FnOnce(&[u8]) -> Result<T, DbError>,
) -> Result<T, DbError> {
    let aligned;
    let bytes = if checkpoint_alignment_copy_bytes(bytes) == 0 {
        bytes
    } else {
        let mut copy = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        copy.extend_from_slice(bytes);
        aligned = copy;
        &aligned
    };
    decode(bytes)
}

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
    ordered_input_fingerprints: Option<[[u8; 32]; 2]>,
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

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct IntervalVnodeCheckpoint {
    core: JoinStateCheckpoint,
    left_normalizer: Option<BoundedJoinInputCheckpoint>,
    right_normalizer: Option<BoundedJoinInputCheckpoint>,
}

struct IntervalVnodeCheckpointCapture {
    core: IntervalJoinCheckpointCapture,
    left_normalizer: Option<BoundedJoinInputCheckpointCapture>,
    right_normalizer: Option<BoundedJoinInputCheckpointCapture>,
    retained_bytes: usize,
}

impl IntervalVnodeCheckpointCapture {
    const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
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

#[cfg(feature = "cluster")]
impl CapturedIntervalCluster {
    fn retained_bytes(&self) -> Result<usize, DbError> {
        let allocation = |bytes: usize| {
            bytes
                .checked_add(usize::from(bytes != 0) * HEAP_ALLOCATION_CHARGE)
                .ok_or_else(|| {
                    DbError::Checkpoint("interval join channel capture accounting overflow".into())
                })
        };
        let mut bytes = 0usize;
        for channels in &self.channels {
            bytes = bytes
                .checked_add(allocation(
                    channels
                        .capacity()
                        .checked_mul(std::mem::size_of::<CapturedIntervalChannel>())
                        .ok_or_else(|| {
                            DbError::Checkpoint(
                                "interval join channel capture accounting overflow".into(),
                            )
                        })?,
                )?)
                .ok_or_else(|| {
                    DbError::Checkpoint("interval join channel capture accounting overflow".into())
                })?;
            for channel in channels {
                bytes = bytes
                    .checked_add(allocation(
                        channel
                            .events
                            .capacity()
                            .checked_mul(std::mem::size_of::<CapturedIntervalEvent>())
                            .ok_or_else(|| {
                                DbError::Checkpoint(
                                    "interval join event capture accounting overflow".into(),
                                )
                            })?,
                    )?)
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
                                    "interval join retained shuffle accounting overflow".into(),
                                )
                            })?)
                            .and_then(|bytes| bytes.checked_add(RETAINED_BATCH_ARC_CHARGE))
                            .ok_or_else(|| {
                                DbError::Checkpoint(
                                    "interval join retained shuffle accounting overflow".into(),
                                )
                            })?;
                    }
                }
            }
        }
        Ok(bytes)
    }
}

struct IntervalJoinOperatorCheckpointCapture {
    checkpoint: IntervalJoinOperatorCheckpoint,
    #[cfg(feature = "cluster")]
    cluster: Option<CapturedIntervalCluster>,
    retained_bytes: u64,
}

struct IntervalWholeRestorePreflight {
    decoded_checkpoint: usize,
    runtime_scratch: usize,
    encoded_frame: usize,
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
            let extra: Result<usize, DbError> = self
                .cluster
                .as_ref()
                .map_or(Ok(0usize), CapturedIntervalCluster::retained_bytes);
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
                let requested_channel_bytes = allocation(
                    cluster.channels[port].len(),
                    std::mem::size_of::<IntervalCheckpointChannel>(),
                )?;
                remaining = remaining
                    .checked_sub(requested_channel_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "{context}: encoded channel roster requires {requested_channel_bytes} bytes"
                        ))
                    })?;
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
                remaining = remaining
                    .checked_sub(channel_bytes.saturating_sub(requested_channel_bytes))
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "{context}: encoded channel roster allocation requires {channel_bytes} bytes"
                        ))
                    })?;
                for channel in &cluster.channels[port] {
                    let mut events = Vec::new();
                    let requested_event_bytes = allocation(
                        channel.events.len(),
                        std::mem::size_of::<IntervalCheckpointEvent>(),
                    )?;
                    remaining = remaining
                        .checked_sub(requested_event_bytes)
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "{context}: peer {} encoded event roster requires {requested_event_bytes} bytes",
                                channel.peer
                            ))
                        })?;
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
                    remaining = remaining
                        .checked_sub(event_bytes.saturating_sub(requested_event_bytes))
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "{context}: peer {} encoded event allocation requires {event_bytes} bytes",
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
                                let requested_route_bytes = allocation(
                                    retained.routed_vnodes().len(),
                                    std::mem::size_of::<u32>(),
                                )?;
                                remaining = remaining
                                    .checked_sub(requested_route_bytes)
                                    .ok_or_else(|| {
                                        DbError::Checkpoint(format!(
                                            "{context}: peer {} route roster requires {requested_route_bytes} bytes",
                                            channel.peer
                                        ))
                                    })?;
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
                                remaining = remaining
                                    .checked_sub(route_bytes.saturating_sub(requested_route_bytes))
                                    .ok_or_else(|| {
                                        DbError::Checkpoint(format!(
                                            "{context}: peer {} route allocation requires {route_bytes} bytes",
                                            channel.peer
                                        ))
                                    })?;
                                routed_vnodes.extend_from_slice(retained.routed_vnodes());
                                let ipc_budget = remaining
                                    .checked_sub(HEAP_ALLOCATION_CHARGE)
                                    .ok_or_else(|| {
                                        DbError::Checkpoint(format!(
                                            "{context}: peer {} queued data has no IPC allocation headroom",
                                            channel.peer
                                        ))
                                    })?;
                                let ipc =
                                    laminar_core::serialization::serialize_batches_stream_bounded(
                                        retained.batch().schema().as_ref(),
                                        std::iter::once(retained.batch()),
                                        ipc_budget,
                                    )
                                    .map_err(|error| {
                                        DbError::Checkpoint(format!(
                                            "{context}: peer {} queued data serialization: {error}",
                                            channel.peer
                                        ))
                                    })?;
                                let ipc_bytes = ipc
                                    .capacity()
                                    .checked_add(
                                        usize::from(ipc.capacity() != 0) * HEAP_ALLOCATION_CHARGE,
                                    )
                                    .ok_or_else(|| {
                                        DbError::Checkpoint(format!(
                                            "{context}: queued data byte accounting overflow"
                                        ))
                                    })?;
                                remaining = remaining.checked_sub(ipc_bytes).ok_or_else(|| {
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

        let archive_budget = remaining
            .checked_sub(HEAP_ALLOCATION_CHARGE)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "{context}: whole checkpoint has no archive-allocation headroom"
                ))
            })?;
        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(archive_budget),
        );
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&capture.checkpoint, writer)
            .map(|bytes| bytes.into_inner().into_vec())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "{context}: archive serialization exceeded its {archive_budget}-byte payload headroom: {error}"
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
    replacements: Vec<(u32, Option<Box<IntervalJoinVnodeState>>)>,
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

#[derive(Clone)]
struct OrderedIntervalInputSpec {
    input_schema: SchemaRef,
    visible_schema: SchemaRef,
    event_time_index: usize,
    mode: BoundedJoinInputMode,
    fingerprint: [u8; 32],
}

#[derive(Clone)]
struct OrderedIntervalJoinSpec {
    left: OrderedIntervalInputSpec,
    right: OrderedIntervalInputSpec,
}

struct OrderedIntervalJoinInputs {
    left: BoundedJoinInputNormalizer,
    right: BoundedJoinInputNormalizer,
}

struct IntervalJoinVnodeState {
    core: IntervalJoinState,
    ordered: Option<OrderedIntervalJoinInputs>,
}

impl std::ops::Deref for IntervalJoinVnodeState {
    type Target = IntervalJoinState;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl std::ops::DerefMut for IntervalJoinVnodeState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}

impl IntervalJoinVnodeState {
    fn new_append() -> Self {
        Self {
            core: IntervalJoinState::new(),
            ordered: None,
        }
    }

    fn try_new_ordered(
        vnode: u32,
        spec: &OrderedIntervalJoinSpec,
        absolute_state_cap: usize,
        dynamic_state_limit: usize,
        join_config: &StreamJoinConfig,
        left_prior_cutoff: i64,
        right_prior_cutoff: i64,
    ) -> Result<Self, DbError> {
        let bound_ms = i64::try_from(join_config.time_bound.as_millis()).map_err(|_| {
            DbError::InvalidOperation(
                "interval join time bound exceeds the supported millisecond range".into(),
            )
        })?;
        let left_preflight = BoundedJoinInputNormalizer::construction_preflight_bytes(
            spec.left.input_schema.as_ref(),
            spec.left.event_time_index,
            &spec.left.mode,
        )?;
        let right_preflight = BoundedJoinInputNormalizer::construction_preflight_bytes(
            spec.right.input_schema.as_ref(),
            spec.right.event_time_index,
            &spec.right.mode,
        )?;
        let core_preflight = IntervalJoinState::weighted_empty_state_preflight(
            spec.left.input_schema.as_ref(),
            spec.right.input_schema.as_ref(),
        )?;
        let projected = HEAP_ALLOCATION_CHARGE
            .checked_add(left_preflight)
            .and_then(|bytes| bytes.checked_add(right_preflight))
            .and_then(|bytes| bytes.checked_add(core_preflight))
            .ok_or_else(|| {
                DbError::BackpressureFail(
                    "ordered interval vnode construction accounting overflow".into(),
                )
            })?;
        if projected > dynamic_state_limit {
            return Err(DbError::BackpressureFail(format!(
                "ordered interval vnode {vnode} construction requires {projected} bytes; dynamic shard limit is {dynamic_state_limit} bytes"
            )));
        }
        let left_limit = dynamic_state_limit
            .checked_sub(HEAP_ALLOCATION_CHARGE)
            .and_then(|bytes| bytes.checked_sub(core_preflight))
            .and_then(|bytes| bytes.checked_sub(right_preflight))
            .expect("construction preflight validated left headroom");
        let left = BoundedJoinInputNormalizer::try_new_at_cutoff(
            Arc::clone(&spec.left.input_schema),
            BoundedJoinInputConfig {
                vnode,
                event_time_index: spec.left.event_time_index,
                mode: spec.left.mode.clone(),
                max_retained_bytes: absolute_state_cap,
            },
            left_prior_cutoff,
            left_limit,
        )?;
        let right_limit = dynamic_state_limit
            .checked_sub(HEAP_ALLOCATION_CHARGE)
            .and_then(|bytes| bytes.checked_sub(core_preflight))
            .and_then(|bytes| bytes.checked_sub(left.accounted_state_bytes()))
            .expect("construction preflight validated right headroom");
        let right = BoundedJoinInputNormalizer::try_new_at_cutoff(
            Arc::clone(&spec.right.input_schema),
            BoundedJoinInputConfig {
                vnode,
                event_time_index: spec.right.event_time_index,
                mode: spec.right.mode.clone(),
                max_retained_bytes: absolute_state_cap,
            },
            right_prior_cutoff,
            right_limit,
        )?;
        let core_limit = dynamic_state_limit
            .checked_sub(HEAP_ALLOCATION_CHARGE)
            .and_then(|bytes| bytes.checked_sub(left.accounted_state_bytes()))
            .and_then(|bytes| bytes.checked_sub(right.accounted_state_bytes()))
            .expect("construction preflight validated core headroom");
        let mut core = IntervalJoinState::new_weighted_at_frontiers(
            left_prior_cutoff,
            right_prior_cutoff,
            bound_ms,
        );
        core.seed_input_schemas(
            Arc::clone(left.visible_schema()),
            Arc::clone(right.visible_schema()),
            join_config,
        )?;
        if core.accounted_state_bytes() > core_limit {
            return Err(DbError::BackpressureFail(format!(
                "ordered interval vnode {vnode} core construction exceeds its {core_limit}-byte cumulative headroom"
            )));
        }
        let state = Self {
            core,
            ordered: Some(OrderedIntervalJoinInputs { left, right }),
        };
        debug_assert!(state.accounted_state_bytes() <= dynamic_state_limit);
        Ok(state)
    }

    fn accounted_state_bytes(&self) -> usize {
        self.core
            .accounted_state_bytes()
            .saturating_add(self.ordered.as_ref().map_or(0, |ordered| {
                HEAP_ALLOCATION_CHARGE
                    .saturating_add(ordered.left.accounted_state_bytes())
                    .saturating_add(ordered.right.accounted_state_bytes())
            }))
    }

    #[cfg(test)]
    fn ordered_fingerprints(&self) -> Option<[[u8; 32]; 2]> {
        self.ordered.as_ref().map(|ordered| {
            [
                ordered.left.config_fingerprint(),
                ordered.right.config_fingerprint(),
            ]
        })
    }

    fn capture_checkpoint(
        &self,
        config: &StreamJoinConfig,
        max_capture_bytes: usize,
    ) -> Result<IntervalVnodeCheckpointCapture, DbError> {
        let core = self.core.capture_checkpoint(config, max_capture_bytes)?;
        let mut remaining = max_capture_bytes
            .checked_sub(core.retained_bytes())
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "interval join core capture exhausted vnode capture headroom".into(),
                )
            })?;
        let (left_normalizer, right_normalizer) = if let Some(ordered) = &self.ordered {
            let left = ordered.left.capture_checkpoint(remaining)?;
            remaining = remaining
                .checked_sub(left.retained_bytes())
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "interval join left normalizer capture exhausted vnode headroom".into(),
                    )
                })?;
            let right = ordered.right.capture_checkpoint(remaining)?;
            (Some(left), Some(right))
        } else {
            (None, None)
        };
        let retained_bytes = core
            .retained_bytes()
            .checked_add(
                left_normalizer
                    .as_ref()
                    .map_or(0, BoundedJoinInputCheckpointCapture::retained_bytes),
            )
            .and_then(|bytes| {
                bytes.checked_add(
                    right_normalizer
                        .as_ref()
                        .map_or(0, BoundedJoinInputCheckpointCapture::retained_bytes),
                )
            })
            .ok_or_else(|| {
                DbError::Checkpoint("interval join vnode capture accounting overflow".into())
            })?;
        Ok(IntervalVnodeCheckpointCapture {
            core,
            left_normalizer,
            right_normalizer,
            retained_bytes,
        })
    }
}

pub(crate) struct IntervalJoinOperator {
    config: StreamJoinConfig,
    key_group_count: KeyGroupCount,
    local_assignment: VnodeAssignmentSnapshot,
    vnode_states: Vec<Option<Box<IntervalJoinVnodeState>>>,
    resident_vnodes: Vec<u32>,
    dirty_vnodes: Vec<bool>,
    dirty_vnode_roster: Vec<u32>,
    full_vnode_capture_required: bool,
    max_managed_state_bytes: usize,
    input_schemas: Option<(SchemaRef, SchemaRef)>,
    ordered_input_spec: Option<OrderedIntervalJoinSpec>,
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
            ordered_input_spec: None,
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

    /// Configure the startup-certified ordered-input contract before vnode state is created.
    pub(crate) fn configure_ordered_inputs(
        &mut self,
        left_mode: BoundedJoinInputMode,
        right_mode: BoundedJoinInputMode,
    ) -> Result<(), DbError> {
        if self.vnode_states.iter().any(Option::is_some) || self.ordered_input_spec.is_some() {
            return Err(DbError::InvalidOperation(
                "ordered interval input must be configured before vnode state exists".into(),
            ));
        }
        let (left_schema, right_schema) = self.input_schemas.as_ref().ok_or_else(|| {
            DbError::Config("ordered interval input requires both declared source schemas".into())
        })?;

        let build = |side: &str,
                     schema: &SchemaRef,
                     time_column: &str,
                     join_keys: &[String],
                     mode: BoundedJoinInputMode|
         -> Result<OrderedIntervalInputSpec, DbError> {
            let event_time_index = schema.index_of(time_column).map_err(|error| {
                DbError::Config(format!(
                    "ordered interval {side} event-time column '{time_column}': {error}"
                ))
            })?;
            if let BoundedJoinInputMode::KeyedUpsert {
                primary_key_indices,
            } = &mode
            {
                for key in join_keys {
                    let index = schema.index_of(key).map_err(|error| {
                        DbError::Config(format!(
                            "ordered interval {side} join key '{key}': {error}"
                        ))
                    })?;
                    if !primary_key_indices.contains(&index) {
                        return Err(DbError::Config(format!(
                            "ordered interval {side} keyed primary key must include join key '{key}'"
                        )));
                    }
                }
                if !primary_key_indices.contains(&event_time_index) {
                    return Err(DbError::Config(format!(
                        "ordered interval {side} keyed primary key must include event time"
                    )));
                }
            }
            let fingerprint =
                normalizer_config_fingerprint(schema.as_ref(), event_time_index, &mode);
            let normalizer = BoundedJoinInputNormalizer::try_new(
                Arc::clone(schema),
                BoundedJoinInputConfig {
                    vnode: 0,
                    event_time_index,
                    mode: mode.clone(),
                    max_retained_bytes: self.max_managed_state_bytes,
                },
            )?;
            Ok(OrderedIntervalInputSpec {
                input_schema: Arc::clone(schema),
                visible_schema: Arc::clone(normalizer.visible_schema()),
                event_time_index,
                mode,
                fingerprint,
            })
        };
        self.ordered_input_spec = Some(OrderedIntervalJoinSpec {
            left: build(
                "left",
                left_schema,
                &self.config.left_time_column,
                &self.config.left_keys,
                left_mode,
            )?,
            right: build(
                "right",
                right_schema,
                &self.config.right_time_column,
                &self.config.right_keys,
                right_mode,
            )?,
        });
        Ok(())
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
    fn try_remote_owner_peers(
        assignment: &VnodeAssignmentSnapshot,
        self_id: NodeId,
        context: &str,
    ) -> Result<Vec<u64>, DbError> {
        let mut peers = Vec::new();
        peers
            .try_reserve_exact(assignment.owners().len())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "{context}: peer-roster reservation failed: {error}"
                ))
            })?;
        for owner in assignment.owners() {
            if !owner.is_unassigned() && *owner != self_id {
                peers.push(owner.0);
            }
        }
        peers.sort_unstable();
        peers.dedup();
        Ok(peers)
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
            .saturating_mul(std::mem::size_of::<Option<Box<IntervalJoinVnodeState>>>())
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
            .saturating_mul(std::mem::size_of::<(
                u32,
                Option<Box<IntervalJoinVnodeState>>,
            )>())
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
        let assignment_identity = self.checkpoint_assignment_identity(max_capture_bytes)?;
        let peer_roster_is_canonical = peers.windows(2).all(|pair| pair[0] < pair[1])
            && peers
                .iter()
                .all(|peer| *peer != 0 && *peer != config.self_id.0)
            && assignment.owners().iter().all(|owner| {
                owner.is_unassigned()
                    || *owner == config.self_id
                    || peers.binary_search(&owner.0).is_ok()
            });
        if self.pending_cluster_input.is_some()
            || self.last_broadcasts != self.local_frontiers
            || !peer_roster_is_canonical
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
        let mut max_declared_vnodes = 0usize;
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
                        max_declared_vnodes =
                            max_declared_vnodes.max(batch.retained.routed_vnodes().len());
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
        let requested_coverage_scratch =
            allocation(max_declared_vnodes).ok_or_else(|| self.accounting_error())?;
        let requested_peak = requested_retained
            .checked_add(requested_coverage_scratch)
            .ok_or_else(|| self.accounting_error())?;
        if requested_peak > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] channel capture requires {requested_peak} bytes; headroom is {max_capture_bytes} bytes",
                self.projection.op_name
            )));
        }

        let mut coverage_marks = Vec::<u8>::new();
        coverage_marks
            .try_reserve_exact(max_declared_vnodes)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "interval join [{}] channel coverage scratch reservation: {error}",
                    self.projection.op_name
                ))
            })?;
        coverage_marks.resize(max_declared_vnodes, 0);
        let coverage_scratch_bytes =
            allocation(coverage_marks.capacity()).ok_or_else(|| self.accounting_error())?;
        let actual_peak = requested_retained
            .checked_add(coverage_scratch_bytes)
            .ok_or_else(|| self.accounting_error())?;
        if actual_peak > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] channel capture allocation requires {actual_peak} bytes; headroom is {max_capture_bytes} bytes",
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
                            let seen = &mut coverage_marks[..declared.len()];
                            seen.fill(0);
                            let coverage_valid = batch.row_vnodes.iter().all(|vnode| {
                                declared.binary_search(vnode).is_ok_and(|index| {
                                    seen[index] = 1;
                                    true
                                })
                            }) && seen.iter().all(|seen| *seen != 0);
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
            owner_map_digest: assignment_identity.1,
            self_id: config.self_id.0,
            recovery_gen: config.receiver.recovery_gen(),
            local_frontiers: self.local_frontiers,
            remote_side_cursor: self.remote_side_cursor,
            remote_peer_cursors: self.remote_peer_cursors,
            channels,
        };
        let retained = capture.retained_bytes()?;
        let retained_peak = retained
            .checked_add(coverage_scratch_bytes)
            .ok_or_else(|| self.accounting_error())?;
        if retained_peak > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] channel capture peak retains {retained_peak} bytes; headroom is {max_capture_bytes} bytes",
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
            && self.ordered_input_spec.is_none()
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
            ordered_input_fingerprints: self
                .ordered_input_spec
                .as_ref()
                .map(|spec| [spec.left.fingerprint, spec.right.fingerprint]),
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
        capture: IntervalVnodeCheckpointCapture,
        context: &str,
        max_encoded_bytes: usize,
    ) -> Result<EncodedStateFrame, DbError> {
        let core = capture.core.encode(max_encoded_bytes)?;
        let mut retained_checkpoint_bytes = core.retained_ipc_bytes()?;
        let left_normalizer = if let Some(capture) = capture.left_normalizer {
            let remaining = max_encoded_bytes
                .checked_sub(retained_checkpoint_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "{context}: core checkpoint exhausted the vnode frame budget"
                    ))
                })?;
            let checkpoint = capture.encode(remaining)?;
            retained_checkpoint_bytes = retained_checkpoint_bytes
                .checked_add(checkpoint.retained_bytes()?)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!("{context}: checkpoint accounting overflow"))
                })?;
            Some(checkpoint)
        } else {
            None
        };
        let right_normalizer = if let Some(capture) = capture.right_normalizer {
            let remaining = max_encoded_bytes
                .checked_sub(retained_checkpoint_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "{context}: left checkpoint exhausted the vnode frame budget"
                    ))
                })?;
            let checkpoint = capture.encode(remaining)?;
            retained_checkpoint_bytes = retained_checkpoint_bytes
                .checked_add(checkpoint.retained_bytes()?)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!("{context}: checkpoint accounting overflow"))
                })?;
            Some(checkpoint)
        } else {
            None
        };
        let checkpoint = IntervalVnodeCheckpoint {
            core,
            left_normalizer,
            right_normalizer,
        };
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
        let core = state.capture_checkpoint(config, max_encoded_bytes)?;
        Self::encode_state_capture(
            IntervalVnodeCheckpointCapture {
                retained_bytes: core.retained_bytes(),
                core,
                left_normalizer: None,
                right_normalizer: None,
            },
            context,
            max_encoded_bytes,
        )
        .map(|bytes| bytes.into_bytes().to_vec())
    }

    fn deserialize_state(
        bytes: &[u8],
        vnode: u32,
        config: &StreamJoinConfig,
        ordered_spec: Option<&OrderedIntervalJoinSpec>,
        context: &str,
        max_state_bytes: usize,
        absolute_state_cap: usize,
        cut: Option<IntervalHandoffCut>,
    ) -> Result<IntervalJoinVnodeState, DbError> {
        let aligned;
        let bytes = if checkpoint_alignment_copy_bytes(bytes) == 0 {
            bytes
        } else {
            let mut copy = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
            copy.extend_from_slice(bytes);
            aligned = copy;
            &aligned
        };
        let archived = rkyv::access::<ArchivedIntervalVnodeCheckpoint, rkyv::rancor::Error>(bytes)
            .map_err(|error| DbError::Checkpoint(format!("{context}: {error}")))?;
        if archived.left_normalizer.is_some() != ordered_spec.is_some()
            || archived.right_normalizer.is_some() != ordered_spec.is_some()
            || archived.core.weighted != ordered_spec.is_some()
        {
            return Err(DbError::Checkpoint(format!(
                "{context}: archived vnode execution contract changed"
            )));
        }
        let deep_decode_preflight = bytes
            .len()
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(4 * HEAP_ALLOCATION_CHARGE))
            .ok_or_else(|| DbError::Checkpoint(format!("{context}: decode accounting overflow")))?;
        if deep_decode_preflight > max_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "{context}: archived vnode needs {deep_decode_preflight} bytes of deep-decode headroom; remaining limit is {max_state_bytes} bytes"
            )));
        }
        let checkpoint = rkyv::from_bytes::<IntervalVnodeCheckpoint, rkyv::rancor::Error>(bytes)
            .map_err(|error| DbError::Checkpoint(format!("{context}: {error}")))?;
        let core_checkpoint_bytes = checkpoint.core.retained_ipc_bytes()?;
        let left_checkpoint_bytes = checkpoint
            .left_normalizer
            .as_ref()
            .map_or(Ok(0), BoundedJoinInputCheckpoint::retained_bytes)?;
        let right_checkpoint_bytes = checkpoint
            .right_normalizer
            .as_ref()
            .map_or(Ok(0), BoundedJoinInputCheckpoint::retained_bytes)?;
        let decoded_checkpoint_bytes = core_checkpoint_bytes
            .checked_add(left_checkpoint_bytes)
            .and_then(|bytes| bytes.checked_add(right_checkpoint_bytes))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<IntervalVnodeCheckpoint>()))
            .ok_or_else(|| {
                DbError::Checkpoint(format!("{context}: decoded checkpoint accounting overflow"))
            })?;
        if decoded_checkpoint_bytes > deep_decode_preflight
            || decoded_checkpoint_bytes > max_state_bytes
        {
            return Err(DbError::Checkpoint(format!(
                "{context}: decoded vnode checkpoint exceeds its preflighted restore headroom"
            )));
        }
        let wrapper_charge = usize::from(ordered_spec.is_some()) * HEAP_ALLOCATION_CHARGE;
        let mut remaining = max_state_bytes
            .checked_sub(decoded_checkpoint_bytes)
            .and_then(|bytes| bytes.checked_sub(wrapper_charge))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "{context}: decoded checkpoint and vnode wrapper exceed the restore limit"
                ))
            })?;
        let mut core = if ordered_spec.is_some() {
            IntervalJoinState::from_weighted_checkpoint(&checkpoint.core, config, remaining)
        } else {
            IntervalJoinState::from_checkpoint(&checkpoint.core, config, remaining)
        }
        .map_err(|error| DbError::Checkpoint(format!("{context}: {error}")))?;
        remaining = remaining
            .checked_sub(core.accounted_state_bytes())
            .ok_or_else(|| DbError::Checkpoint(format!("{context}: core restore overflow")))?;

        let ordered = match (
            ordered_spec,
            checkpoint.left_normalizer.as_ref(),
            checkpoint.right_normalizer.as_ref(),
        ) {
            (None, None, None) => None,
            (Some(spec), Some(left_checkpoint), Some(right_checkpoint)) => {
                let left = BoundedJoinInputNormalizer::from_checkpoint(
                    left_checkpoint,
                    Arc::clone(&spec.left.input_schema),
                    BoundedJoinInputConfig {
                        vnode,
                        event_time_index: spec.left.event_time_index,
                        mode: spec.left.mode.clone(),
                        max_retained_bytes: absolute_state_cap,
                    },
                    remaining,
                )?;
                remaining = remaining
                    .checked_sub(left.accounted_state_bytes())
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!("{context}: left normalizer restore overflow"))
                    })?;
                let right = BoundedJoinInputNormalizer::from_checkpoint(
                    right_checkpoint,
                    Arc::clone(&spec.right.input_schema),
                    BoundedJoinInputConfig {
                        vnode,
                        event_time_index: spec.right.event_time_index,
                        mode: spec.right.mode.clone(),
                        max_retained_bytes: absolute_state_cap,
                    },
                    remaining,
                )?;
                core.seed_input_schemas(
                    Arc::clone(left.visible_schema()),
                    Arc::clone(right.visible_schema()),
                    config,
                )?;
                Some(OrderedIntervalJoinInputs { left, right })
            }
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "{context}: vnode execution contract does not match its normalizer frames"
                )));
            }
        };
        let state = IntervalJoinVnodeState { core, ordered };
        if let Some(ordered) = &state.ordered {
            let normalizer_cut = IntervalHandoffCut {
                left_watermark: ordered.left.closed_cutoff(),
                right_watermark: ordered.right.closed_cutoff(),
                #[cfg(feature = "cluster")]
                left_idle: false,
                #[cfg(feature = "cluster")]
                right_idle: false,
            };
            if cut.is_some_and(|cut| {
                cut.left_watermark != normalizer_cut.left_watermark
                    || cut.right_watermark != normalizer_cut.right_watermark
            }) {
                return Err(DbError::Checkpoint(format!(
                    "{context}: ordered normalizer cutoffs disagree with the whole handoff cut"
                )));
            }
            Self::validate_ordered_core_cutoffs(&state, config, context, normalizer_cut)?;
        } else if let Some(cut) = cut {
            Self::validate_handoff_cutoffs(
                checkpoint.core.left_evicted_cutoff,
                checkpoint.core.right_evicted_cutoff,
                checkpoint.core.left_buffer_rows != 0,
                checkpoint.core.right_buffer_rows != 0,
                config,
                context,
                cut,
            )?;
        }
        if state.accounted_state_bytes() > max_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "{context}: combined vnode state exceeds its {max_state_bytes}-byte restore limit"
            )));
        }
        Ok(state)
    }

    fn decode_vnode_frame(
        bytes: &[u8],
        vnode: u32,
        config: &StreamJoinConfig,
        ordered_spec: Option<&OrderedIntervalJoinSpec>,
        context: &str,
        max_state_bytes: usize,
        absolute_state_cap: usize,
        cut: Option<IntervalHandoffCut>,
    ) -> Result<Option<IntervalJoinVnodeState>, DbError> {
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
            PRESENT_VNODE => Self::deserialize_state(
                payload,
                vnode,
                config,
                ordered_spec,
                context,
                max_state_bytes,
                absolute_state_cap,
                cut,
            )
            .map(Some),
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

    fn validate_ordered_core_cutoffs(
        state: &IntervalJoinVnodeState,
        config: &StreamJoinConfig,
        context: &str,
        cut: IntervalHandoffCut,
    ) -> Result<(), DbError> {
        let bound_ms = i64::try_from(config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "{context}: configured time bound exceeds the supported millisecond range"
            ))
        })?;
        let expected = (
            cut.right_watermark.saturating_sub(bound_ms),
            cut.left_watermark,
        );
        if state.evicted_cutoffs() != expected {
            return Err(DbError::Checkpoint(format!(
                "{context}: weighted core cutoffs disagree with authoritative normalizer cutoffs"
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
            || checkpoint.ordered_input_fingerprints
                != self
                    .ordered_input_spec
                    .as_ref()
                    .map(|spec| [spec.left.fingerprint, spec.right.fingerprint])
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

    fn validate_archived_checkpoint_config(
        &self,
        checkpoint: &ArchivedIntervalJoinOperatorCheckpoint,
    ) -> Result<(), DbError> {
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "interval join [{}] configured bound is not checkpointable",
                self.projection.op_name
            ))
        })?;
        let expected_fingerprints = self
            .ordered_input_spec
            .as_ref()
            .map(|spec| [spec.left.fingerprint, spec.right.fingerprint]);
        let fingerprints_match = match (
            checkpoint.ordered_input_fingerprints.as_ref(),
            expected_fingerprints.as_ref(),
        ) {
            (None, None) => true,
            (Some(archived), Some(expected)) => archived.as_slice() == expected.as_slice(),
            _ => false,
        };
        let left_keys_match = checkpoint.left_keys.len() == self.config.left_keys.len()
            && checkpoint
                .left_keys
                .iter()
                .zip(&self.config.left_keys)
                .all(|(archived, expected)| archived.as_str() == expected.as_str());
        let right_keys_match = checkpoint.right_keys.len() == self.config.right_keys.len()
            && checkpoint
                .right_keys
                .iter()
                .zip(&self.config.right_keys)
                .all(|(archived, expected)| archived.as_str() == expected.as_str());
        if checkpoint.version != OPERATOR_CHECKPOINT_VERSION
            || !fingerprints_match
            || checkpoint.join_type != join_type_tag(self.config.join_type)
            || !left_keys_match
            || !right_keys_match
            || checkpoint.left_time_column.as_str() != self.config.left_time_column.as_str()
            || checkpoint.right_time_column.as_str() != self.config.right_time_column.as_str()
            || checkpoint.left_table.as_str() != self.config.left_table.as_str()
            || checkpoint.right_table.as_str() != self.config.right_table.as_str()
            || checkpoint.bound_ms != bound_ms
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] archived checkpoint version or configuration does not match the operator",
                self.projection.op_name
            )));
        }
        Ok(())
    }

    fn preflight_whole_restore_archive(
        &self,
        bytes: &Vec<u8>,
    ) -> Result<IntervalWholeRestorePreflight, DbError> {
        let alignment_copy_bytes = checkpoint_alignment_copy_bytes(bytes);
        let encoded_bytes = bytes
            .capacity()
            .checked_add(usize::from(bytes.capacity() != 0) * HEAP_ALLOCATION_CHARGE)
            .and_then(|bytes| bytes.checked_add(alignment_copy_bytes))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] whole restore accounting overflow",
                    self.projection.op_name
                ))
            })?;
        let encoded_peak = self
            .accounted_state_bytes()
            .checked_add(encoded_bytes)
            .ok_or_else(|| self.accounting_error())?;
        if encoded_peak > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "interval join [{}] whole checkpoint restore payload",
                    self.projection.op_name
                ),
                accounted_bytes: encoded_peak,
                limit_bytes: self.max_managed_state_bytes,
            });
        }

        let aligned;
        let bytes = if alignment_copy_bytes == 0 {
            bytes.as_slice()
        } else {
            let mut copy = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
            copy.extend_from_slice(bytes);
            aligned = copy;
            &aligned
        };
        let archived =
            rkyv::access::<ArchivedIntervalJoinOperatorCheckpoint, rkyv::rancor::Error>(bytes)
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] checkpoint archive validation: {error}",
                        self.projection.op_name
                    ))
                })?;
        self.validate_archived_checkpoint_config(archived)?;

        let allocation = |payload: usize| {
            payload
                .checked_add(usize::from(payload != 0) * HEAP_ALLOCATION_CHARGE)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] whole restore accounting overflow",
                        self.projection.op_name
                    ))
                })
        };
        let roster = |count: usize, item_bytes: usize| {
            count
                .checked_mul(item_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] whole restore roster overflow",
                        self.projection.op_name
                    ))
                })
                .and_then(allocation)
        };
        let add = |total: &mut usize, charge: usize| {
            *total = total.checked_add(charge).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] whole restore accounting overflow",
                    self.projection.op_name
                ))
            })?;
            Ok::<(), DbError>(())
        };

        let mut decoded_checkpoint_bytes = std::mem::size_of::<IntervalJoinOperatorCheckpoint>();
        add(
            &mut decoded_checkpoint_bytes,
            roster(archived.left_keys.len(), std::mem::size_of::<String>())?,
        )?;
        add(
            &mut decoded_checkpoint_bytes,
            roster(archived.right_keys.len(), std::mem::size_of::<String>())?,
        )?;
        for key in archived.left_keys.iter().chain(archived.right_keys.iter()) {
            add(&mut decoded_checkpoint_bytes, allocation(key.len())?)?;
        }
        for value in [
            &archived.left_time_column,
            &archived.right_time_column,
            &archived.left_table,
            &archived.right_table,
        ] {
            add(&mut decoded_checkpoint_bytes, allocation(value.len())?)?;
        }

        #[cfg(feature = "cluster")]
        let mut runtime_bytes = 0usize;
        #[cfg(not(feature = "cluster"))]
        let runtime_bytes = 0usize;
        #[cfg(feature = "cluster")]
        match (self.cluster_shuffle.as_ref(), archived.cluster.as_ref()) {
            (Some(config), Some(cluster)) => {
                let (_, assignment, peers) = self.active_cluster_scope()?;
                let owner_map_digest =
                    laminar_core::checkpoint::CheckpointAssignmentFence::owner_map_digest_iter(
                        u32::from(self.key_group_count),
                        assignment.owners().iter().map(|owner| owner.0),
                    );
                if cluster.assignment_version != assignment.version()
                    || cluster.owner_map_digest != owner_map_digest
                    || cluster.self_id != config.self_id.0
                    || cluster.recovery_gen > config.receiver.recovery_gen()
                    || cluster.remote_side_cursor > 1
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] archived cluster checkpoint identity is invalid",
                        self.projection.op_name
                    )));
                }
                for port in 0..2 {
                    let channels = &cluster.channels[port];
                    if channels.len() != peers.len()
                        || !channels
                            .iter()
                            .map(|channel| channel.peer)
                            .eq(peers.iter().copied())
                    {
                        return Err(DbError::Checkpoint(format!(
                            "interval join [{}] archived cluster channel roster is invalid",
                            self.projection.op_name
                        )));
                    }
                    add(
                        &mut decoded_checkpoint_bytes,
                        roster(
                            channels.len(),
                            std::mem::size_of::<IntervalCheckpointChannel>(),
                        )?,
                    )?;
                    add(
                        &mut runtime_bytes,
                        roster(
                            channels.len(),
                            std::mem::size_of::<(u64, IntervalPeerChannel)>()
                                .saturating_add(PEER_CHANNEL_ENTRY_CHARGE),
                        )?,
                    )?;
                    for channel in channels.iter() {
                        add(
                            &mut decoded_checkpoint_bytes,
                            roster(
                                channel.events.len(),
                                std::mem::size_of::<IntervalCheckpointEvent>(),
                            )?,
                        )?;
                        add(
                            &mut runtime_bytes,
                            roster(channel.events.len(), REMOTE_EVENT_CHARGE)?,
                        )?;
                        for event in channel.events.iter() {
                            if let ArchivedIntervalCheckpointEvent::Data {
                                routed_vnodes,
                                ipc,
                                ..
                            } = event
                            {
                                let routes = routed_vnodes.as_slice();
                                if routes.is_empty()
                                    || routes
                                        .windows(2)
                                        .any(|pair| pair[0].to_native() >= pair[1].to_native())
                                    || routes.iter().any(|vnode| {
                                        let vnode = vnode.to_native();
                                        vnode >= u32::from(self.key_group_count)
                                            || assignment
                                                .owners()
                                                .get(vnode as usize)
                                                .is_none_or(|owner| *owner != config.self_id)
                                    })
                                {
                                    return Err(DbError::Checkpoint(format!(
                                        "interval join [{}] archived queued-data vnode roster is invalid",
                                        self.projection.op_name
                                    )));
                                }
                                let ipc_preflight =
                                    preflight_queued_batch_ipc_restore(ipc.as_slice())?;
                                if ipc_preflight.rows == 0
                                    || routes.len() > ipc_preflight.rows
                                    || ipc_preflight.body_bytes > ipc.len()
                                {
                                    return Err(DbError::Checkpoint(format!(
                                        "interval join [{}] archived queued-data IPC shape is invalid",
                                        self.projection.op_name
                                    )));
                                }
                                add(
                                    &mut decoded_checkpoint_bytes,
                                    roster(routes.len(), std::mem::size_of::<u32>())?,
                                )?;
                                add(&mut decoded_checkpoint_bytes, allocation(ipc.len())?)?;

                                let decoded_payload = ipc
                                    .len()
                                    .checked_mul(8)
                                    .ok_or_else(|| self.accounting_error())?;
                                let row_scratch = ipc_preflight
                                    .rows
                                    .checked_mul(WHOLE_RESTORE_ROW_SCRATCH_CHARGE)
                                    .ok_or_else(|| self.accounting_error())?;
                                let route_scratch = routes
                                    .len()
                                    .checked_add(ipc_preflight.rows)
                                    .and_then(|count| count.checked_mul(std::mem::size_of::<u32>()))
                                    .and_then(|bytes| bytes.checked_add(2 * ROW_VNODE_ARC_CHARGE))
                                    .ok_or_else(|| self.accounting_error())?;
                                let event_runtime = decoded_payload
                                    .checked_add(row_scratch)
                                    .and_then(|bytes| bytes.checked_add(route_scratch))
                                    .and_then(|bytes| bytes.checked_add(4 * HEAP_ALLOCATION_CHARGE))
                                    .ok_or_else(|| self.accounting_error())?;
                                add(&mut runtime_bytes, event_runtime)?;
                            }
                        }
                    }
                }
            }
            (None, None) => {}
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] archived checkpoint deployment mode does not match the operator",
                    self.projection.op_name
                )));
            }
        }
        #[cfg(not(feature = "cluster"))]
        if archived.cluster.is_some() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] archived checkpoint contains cluster channel state",
                self.projection.op_name
            )));
        }

        let generic_decode_bound = bytes
            .len()
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(8 * HEAP_ALLOCATION_CHARGE))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] whole restore decode accounting overflow",
                    self.projection.op_name
                ))
            })?;
        decoded_checkpoint_bytes = decoded_checkpoint_bytes.max(generic_decode_bound);
        let restore_peak = self
            .accounted_state_bytes()
            .checked_add(encoded_bytes)
            .and_then(|bytes| bytes.checked_add(decoded_checkpoint_bytes))
            .and_then(|bytes| bytes.checked_add(runtime_bytes))
            .ok_or_else(|| self.accounting_error())?;
        if restore_peak > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "interval join [{}] whole checkpoint restore preflight",
                    self.projection.op_name
                ),
                accounted_bytes: restore_peak,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        Ok(IntervalWholeRestorePreflight {
            decoded_checkpoint: decoded_checkpoint_bytes,
            runtime_scratch: runtime_bytes,
            encoded_frame: encoded_bytes,
        })
    }

    fn decoded_whole_checkpoint_bytes(
        checkpoint: &IntervalJoinOperatorCheckpoint,
    ) -> Result<usize, DbError> {
        let allocation = |bytes: usize| {
            bytes
                .checked_add(usize::from(bytes != 0) * HEAP_ALLOCATION_CHARGE)
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "interval join decoded whole-checkpoint accounting overflow".into(),
                    )
                })
        };
        let roster = |capacity: usize, item_bytes: usize| {
            capacity
                .checked_mul(item_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "interval join decoded whole-checkpoint roster overflow".into(),
                    )
                })
                .and_then(allocation)
        };
        let mut bytes = std::mem::size_of::<IntervalJoinOperatorCheckpoint>();
        for charge in [
            roster(
                checkpoint.left_keys.capacity(),
                std::mem::size_of::<String>(),
            )?,
            roster(
                checkpoint.right_keys.capacity(),
                std::mem::size_of::<String>(),
            )?,
            allocation(checkpoint.left_time_column.capacity())?,
            allocation(checkpoint.right_time_column.capacity())?,
            allocation(checkpoint.left_table.capacity())?,
            allocation(checkpoint.right_table.capacity())?,
        ] {
            bytes = bytes.checked_add(charge).ok_or_else(|| {
                DbError::Checkpoint(
                    "interval join decoded whole-checkpoint accounting overflow".into(),
                )
            })?;
        }
        for key in checkpoint.left_keys.iter().chain(&checkpoint.right_keys) {
            bytes = bytes
                .checked_add(allocation(key.capacity())?)
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "interval join decoded whole-checkpoint accounting overflow".into(),
                    )
                })?;
        }
        if let Some(cluster) = &checkpoint.cluster {
            for channels in &cluster.channels {
                bytes = bytes
                    .checked_add(roster(
                        channels.capacity(),
                        std::mem::size_of::<IntervalCheckpointChannel>(),
                    )?)
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join decoded whole-checkpoint accounting overflow".into(),
                        )
                    })?;
                for channel in channels {
                    bytes = bytes
                        .checked_add(roster(
                            channel.events.capacity(),
                            std::mem::size_of::<IntervalCheckpointEvent>(),
                        )?)
                        .ok_or_else(|| {
                            DbError::Checkpoint(
                                "interval join decoded whole-checkpoint accounting overflow".into(),
                            )
                        })?;
                    for event in &channel.events {
                        if let IntervalCheckpointEvent::Data {
                            routed_vnodes, ipc, ..
                        } = event
                        {
                            let route_charge =
                                roster(routed_vnodes.capacity(), std::mem::size_of::<u32>())?;
                            let ipc_charge = allocation(ipc.capacity())?;
                            bytes = bytes
                                .checked_add(route_charge)
                                .and_then(|bytes| bytes.checked_add(ipc_charge))
                                .ok_or_else(|| {
                                    DbError::Checkpoint(
                                        "interval join decoded whole-checkpoint accounting overflow"
                                            .into(),
                                    )
                                })?;
                        }
                    }
                }
            }
        }
        Ok(bytes)
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_assignment_identity(
        &self,
        max_scratch_bytes: usize,
    ) -> Result<(u64, [u8; 32], u64), DbError> {
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] has no cluster assignment",
                self.projection.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let requested_owner_bytes = assignment
            .owners()
            .len()
            .checked_mul(std::mem::size_of::<u64>())
            .and_then(|bytes| bytes.checked_add(usize::from(bytes != 0) * HEAP_ALLOCATION_CHARGE))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] checkpoint owner-roster accounting overflow",
                    self.projection.op_name
                ))
            })?;
        if requested_owner_bytes > max_scratch_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint owner roster needs {requested_owner_bytes} bytes; scratch headroom is {max_scratch_bytes} bytes",
                self.projection.op_name
            )));
        }
        let mut owners = Vec::new();
        owners
            .try_reserve_exact(assignment.owners().len())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "interval join [{}] checkpoint owner-roster reservation failed: {error}",
                    self.projection.op_name
                ))
            })?;
        let actual_owner_bytes = owners
            .capacity()
            .checked_mul(std::mem::size_of::<u64>())
            .and_then(|bytes| bytes.checked_add(usize::from(bytes != 0) * HEAP_ALLOCATION_CHARGE))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] checkpoint owner-roster accounting overflow",
                    self.projection.op_name
                ))
            })?;
        if actual_owner_bytes > max_scratch_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint owner allocation needs {actual_owner_bytes} bytes; scratch headroom is {max_scratch_bytes} bytes",
                self.projection.op_name
            )));
        }
        owners.extend(assignment.owners().iter().map(|owner| owner.0));
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
        max_decode_bytes: usize,
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
        let predecessor_owners: &[NodeId] = match transition.mode {
            ManagedVnodeTransitionMode::Live => self.local_assignment.owners(),
            ManagedVnodeTransitionMode::CheckpointBootstrap { predecessor_owners } => {
                predecessor_owners
            }
        };

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
            with_aligned_checkpoint_bytes(restore.state, |state| {
                rkyv::access::<ArchivedIntervalJoinOperatorCheckpoint, rkyv::rancor::Error>(state)
                    .map(|_| ())
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] donor {} whole checkpoint archive: {error}",
                            self.projection.op_name, restore.participant_id
                        ))
                    })
            })?;
            let decode_preflight = restore
                .state
                .len()
                .checked_mul(4)
                .and_then(|bytes| bytes.checked_add(8 * HEAP_ALLOCATION_CHARGE))
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] donor {} whole decode accounting overflow",
                        self.projection.op_name, restore.participant_id
                    ))
                })?;
            if decode_preflight > max_decode_bytes {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] donor {} whole checkpoint needs {decode_preflight} bytes of decode headroom; remaining transition headroom is {max_decode_bytes} bytes",
                    self.projection.op_name, restore.participant_id
                )));
            }
            let checkpoint = with_aligned_checkpoint_bytes(restore.state, |state| {
                rkyv::from_bytes::<IntervalJoinOperatorCheckpoint, rkyv::rancor::Error>(state)
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] donor {} whole checkpoint: {error}",
                            self.projection.op_name, restore.participant_id
                        ))
                    })
            })?;
            let decoded_bytes = Self::decoded_whole_checkpoint_bytes(&checkpoint)?;
            if decoded_bytes > decode_preflight || decoded_bytes > max_decode_bytes {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] donor {} decoded whole checkpoint exceeds its preflighted transition headroom",
                    self.projection.op_name, restore.participant_id
                )));
            }
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
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] donor {} whole checkpoint is not a portable predecessor cut",
                    self.projection.op_name, restore.participant_id
                )));
            }
            let mut expected_peers = Vec::new();
            expected_peers
                .try_reserve_exact(predecessor_owners.len())
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] donor {} peer-roster reservation failed: {error}",
                        self.projection.op_name, restore.participant_id
                    ))
                })?;
            expected_peers.extend(
                predecessor_owners
                    .iter()
                    .filter(|owner| !owner.is_unassigned() && owner.0 != restore.participant_id)
                    .map(|owner| owner.0),
            );
            expected_peers.sort_unstable();
            expected_peers.dedup();
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
                    "interval join [{}] donor {} whole checkpoint has an invalid side cursor",
                    self.projection.op_name, restore.participant_id
                )));
            }
            for side in [JoinInputSide::Left, JoinInputSide::Right] {
                let port = side.port();
                if cluster.channels[port].len() != expected_peers.len()
                    || !cluster.channels[port]
                        .iter()
                        .map(|channel| channel.peer)
                        .eq(expected_peers.iter().copied())
                    || cluster.channels[port]
                        .iter()
                        .any(|channel| !channel.events.is_empty())
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] donor {} {} portable channel roster is invalid",
                        self.projection.op_name,
                        restore.participant_id,
                        side.name()
                    )));
                }
                for channel in &cluster.channels[port] {
                    let frontier: InputFrontier = channel.applied.into();
                    self.validate_frontier(InputFrontier::default(), frontier, side)?;
                }
                if cluster.remote_peer_cursors[port]
                    .is_some_and(|peer| expected_peers.binary_search(&peer).is_err())
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] donor {} portable remote cursor is invalid",
                        self.projection.op_name, restore.participant_id
                    )));
                }
                let merged = merge_input_frontier_iter(
                    std::iter::once(local_frontiers[port]).chain(
                        cluster.channels[port]
                            .iter()
                            .map(|channel| InputFrontier::from(channel.applied)),
                    ),
                    i64::MIN,
                );
                let (expected_watermark, expected_idle) = if port == 0 {
                    (
                        checkpoint.applied_left_watermark,
                        checkpoint.applied_left_idle,
                    )
                } else {
                    (
                        checkpoint.applied_right_watermark,
                        checkpoint.applied_right_idle,
                    )
                };
                if merged.watermark.unwrap_or(i64::MIN) != expected_watermark
                    || merged.idle != expected_idle
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] donor {} portable applied frontier has no exact drained-cut evidence",
                        self.projection.op_name, restore.participant_id
                    )));
                }
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
        state: &IntervalJoinVnodeState,
        context: &str,
    ) -> Result<Option<IntervalHandoffCut>, DbError> {
        if let Some(ordered) = &state.ordered {
            let cut = IntervalHandoffCut {
                left_watermark: ordered.left.closed_cutoff(),
                right_watermark: ordered.right.closed_cutoff(),
                left_idle: false,
                right_idle: false,
            };
            Self::validate_ordered_core_cutoffs(state, &self.config, context, cut)?;
            return Ok(Some(cut));
        }
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
        let initialized_new = slot.is_none();
        if initialized_new {
            let state = if let Some(spec) = &self.ordered_input_spec {
                IntervalJoinVnodeState::try_new_ordered(
                    vnode,
                    spec,
                    self.max_managed_state_bytes,
                    shard_limit,
                    &self.config,
                    left_admission_watermark,
                    right_admission_watermark,
                )?
            } else {
                let mut state = IntervalJoinVnodeState::new_append();
                if let Some((left_schema, right_schema)) = &self.input_schemas {
                    state.seed_input_schemas(
                        left_schema.clone(),
                        right_schema.clone(),
                        &self.config,
                    )?;
                }
                state
            };
            if state.accounted_state_bytes() > shard_limit {
                return Err(DbError::BackpressureFail(format!(
                    "interval join [{}] cannot allocate vnode {vnode} state within its {shard_limit}-byte remaining limit",
                    self.projection.op_name
                )));
            }
            self.vnode_states[vnode_index] = Some(Box::new(state));
            self.add_resident_vnode(vnode);
        }
        let state = self.vnode_states[vnode_index]
            .as_mut()
            .expect("interval join state initialized");
        let cutoffs_before = state.evicted_cutoffs();
        let ordered_cutoffs_before = state
            .ordered
            .as_ref()
            .map(|ordered| (ordered.left.closed_cutoff(), ordered.right.closed_cutoff()));
        let result = (|| {
            if state.ordered.is_none() {
                execute_interval_join_cycle(
                    &mut state.core,
                    left,
                    right,
                    &self.config,
                    left_admission_watermark,
                    right_admission_watermark,
                    left_watermark,
                    right_watermark,
                    shard_limit,
                    output_budget,
                )
            } else {
                let IntervalJoinVnodeState { core, ordered } = state.as_mut();
                let ordered = ordered
                    .as_mut()
                    .expect("ordered vnode checked before differential execution");
                if ordered.left.closed_cutoff() != left_admission_watermark
                    || ordered.right.closed_cutoff() != right_admission_watermark
                {
                    return Err(DbError::PipelineTerminal(format!(
                        "interval join [{}] vnode {vnode} ordered cutoffs disagree with applied input frontiers",
                        self.projection.op_name
                    )));
                }
                let wrapper_charge = HEAP_ALLOCATION_CHARGE;
                let core_current = core.accounted_state_bytes();
                let right_current = ordered.right.accounted_state_bytes();
                let left_limit = shard_limit
                    .checked_sub(wrapper_charge)
                    .and_then(|bytes| bytes.checked_sub(core_current))
                    .and_then(|bytes| bytes.checked_sub(right_current))
                    .ok_or_else(|| {
                        DbError::BackpressureFail(format!(
                            "interval join [{}] vnode {vnode} ordered state exceeds its shard budget",
                            self.projection.op_name
                        ))
                    })?;
                let left_prepared = ordered.left.prepare_batches(
                    left,
                    left_admission_watermark,
                    left_watermark,
                    left_limit,
                )?;
                let left_projected = left_prepared.projected_state_bytes();
                let left_reserved = left_prepared.transient_state_bytes().max(left_projected);
                let right_limit = shard_limit
                    .checked_sub(wrapper_charge)
                    .and_then(|bytes| bytes.checked_sub(core_current))
                    .and_then(|bytes| bytes.checked_sub(left_reserved))
                    .ok_or_else(|| {
                        DbError::BackpressureFail(format!(
                            "interval join [{}] vnode {vnode} left normalization exhausted its shard budget",
                            self.projection.op_name
                        ))
                    })?;
                let right_prepared = ordered.right.prepare_batches(
                    right,
                    right_admission_watermark,
                    right_watermark,
                    right_limit,
                )?;
                let right_projected = right_prepared.projected_state_bytes();
                let right_reserved = right_prepared.transient_state_bytes().max(right_projected);
                let core_limit = shard_limit
                    .checked_sub(wrapper_charge)
                    .and_then(|bytes| bytes.checked_sub(left_reserved))
                    .and_then(|bytes| bytes.checked_sub(right_reserved))
                    .ok_or_else(|| {
                        DbError::BackpressureFail(format!(
                            "interval join [{}] vnode {vnode} normalization exhausted its shard budget",
                            self.projection.op_name
                        ))
                    })?;
                let result = execute_weighted_interval_join_cycle(
                    core,
                    left_prepared.output_batches(),
                    right_prepared.output_batches(),
                    &self.config,
                    left_watermark,
                    right_watermark,
                    core_limit,
                    output_budget,
                );
                match result {
                    Ok(output) => {
                        left_prepared.commit();
                        right_prepared.commit();
                        Ok(output)
                    }
                    Err(error) => Err(error),
                }
            }
        })();
        *accounted_total = other_state_bytes.saturating_add(state.accounted_state_bytes());
        let checkpoint_state_changed = has_input
            || state.evicted_cutoffs() != cutoffs_before
            || state
                .ordered
                .as_ref()
                .map(|ordered| (ordered.left.closed_cutoff(), ordered.right.closed_cutoff()))
                != ordered_cutoffs_before;
        if result.is_ok() && checkpoint_state_changed {
            self.mark_vnode_dirty(vnode);
        }
        if result.is_err() && initialized_new {
            self.vnode_states[vnode_index] = None;
            self.remove_resident_vnode(vnode);
            *accounted_total = other_state_bytes;
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

    async fn initialize_projection(&mut self) -> Result<(), DbError> {
        if self.projection.is_initialized() {
            return Ok(());
        }
        let projection_input_schema = if let Some(spec) = &self.ordered_input_spec {
            build_weighted_output_schema(
                &spec.left.visible_schema,
                &spec.right.visible_schema,
                &self.config,
            )
        } else {
            let (left_schema, right_schema) = self.input_schemas.as_ref().ok_or_else(|| {
                DbError::Config(format!(
                    "interval join [{}] requires both input schemas before projection initialization",
                    self.projection.op_name
                ))
            })?;
            build_output_schema(left_schema, right_schema, &self.config)
        };
        self.projection.initialize(&projection_input_schema).await
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
            let ordered_spec = self.ordered_input_spec.as_ref().map(|spec| {
                if port == 0 {
                    &spec.left
                } else {
                    &spec.right
                }
            });
            for (batch_index, routed_batch) in inputs.get(port).into_iter().flatten().enumerate() {
                let stripped;
                let batch = if ordered_spec.is_some() {
                    laminar_connectors::connector::source_row_positions(routed_batch)
                        .map_err(|error| {
                            DbError::SchemaMismatch(format!(
                                "interval join [{}] {side} batch {batch_index} source positions: {error}",
                                self.projection.op_name
                            ))
                        })?
                        .ok_or_else(|| {
                            DbError::SchemaMismatch(format!(
                                "interval join [{}] {side} batch {batch_index} requires deterministic source positions",
                                self.projection.op_name
                            ))
                        })?;
                    laminar_connectors::connector::source_mutations_routed(routed_batch).map_err(
                        |error| {
                            DbError::SchemaMismatch(format!(
                                "interval join [{}] {side} batch {batch_index} source mutations: {error}",
                                self.projection.op_name
                            ))
                        },
                    )?;
                    let positioned =
                        laminar_connectors::connector::strip_source_mutations_routed(routed_batch)
                            .map_err(|error| {
                                DbError::SchemaMismatch(format!(
                                    "interval join [{}] {side} batch {batch_index} source mutations: {error}",
                                    self.projection.op_name
                                ))
                            })?;
                    stripped = laminar_connectors::connector::strip_source_row_positions(
                        &positioned,
                    )
                    .map_err(|error| {
                        DbError::SchemaMismatch(format!(
                            "interval join [{}] {side} batch {batch_index} source positions: {error}",
                            self.projection.op_name
                        ))
                    })?;
                    &stripped
                } else {
                    routed_batch
                };
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
        // Publishing a vnode transition deliberately leaves the restored local cut waiting for
        // one bootstrap broadcast. While that broadcast is outstanding `wants_input()` is false,
        // so the graph steps us with empty input even when post-checkpoint source rows are already
        // buffered. Do not let the freshly extracted source frontier overtake those rows: publish
        // the exact restored cut first. Once the send completes, input is admitted together with
        // the then-current source frontier in the following plan.
        let has_data = inputs.iter().flatten().any(|batch| batch.num_rows() != 0);
        let frontiers = if self.last_broadcasts == self.local_frontiers {
            frontiers
        } else {
            if has_data {
                return Err(DbError::InvalidOperation(format!(
                    "interval join [{}] received local input before its restored frontier was broadcast",
                    self.projection.op_name
                )));
            }
            self.local_frontiers
        };
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

    fn accounting_error(&self) -> DbError {
        DbError::Pipeline(format!(
            "interval join [{}] managed-state accounting overflow",
            self.projection.op_name
        ))
    }

    #[cfg(feature = "cluster")]
    fn outbound_finalize_error(&self, error: DbError) -> DbError {
        if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
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
        if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
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
        self.initialize_projection().await
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
        // Live local DDL can install an append-only interval operator after graph startup. Compile
        // its deterministic projection before routing, frontier changes, or any vnode admission.
        self.initialize_projection().await?;

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
        let OperatorCheckpoint { data } = checkpoint;
        if data.len() > self.max_managed_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint payload is {} bytes; restore limit is {} bytes",
                self.projection.op_name,
                data.len(),
                self.max_managed_state_bytes
            )));
        }
        let restore_preflight = self.preflight_whole_restore_archive(&data)?;
        let checkpoint = with_aligned_checkpoint_bytes(&data, |data| {
            rkyv::from_bytes::<IntervalJoinOperatorCheckpoint, rkyv::rancor::Error>(data).map_err(
                |error| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] checkpoint deserialization: {error}",
                        self.projection.op_name
                    ))
                },
            )
        })?;
        drop(data);
        let decoded_checkpoint_bytes = Self::decoded_whole_checkpoint_bytes(&checkpoint)?;
        if decoded_checkpoint_bytes > restore_preflight.decoded_checkpoint {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] decoded whole checkpoint exceeds its preflighted bound",
                self.projection.op_name
            )));
        }
        let decoded_peak = self
            .accounted_state_bytes()
            .checked_add(restore_preflight.encoded_frame)
            .and_then(|bytes| bytes.checked_add(decoded_checkpoint_bytes))
            .and_then(|bytes| bytes.checked_add(restore_preflight.runtime_scratch))
            .ok_or_else(|| self.accounting_error())?;
        if decoded_peak > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "interval join [{}] decoded whole checkpoint restore",
                    self.projection.op_name
                ),
                accounted_bytes: decoded_peak,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        self.validate_checkpoint_config(&checkpoint)?;
        #[cfg(feature = "cluster")]
        let decoded_cluster = match (self.cluster_shuffle.clone(), checkpoint.cluster) {
            (Some(config), Some(cluster)) => {
                let (_, assignment, peers) = self.active_cluster_scope()?;
                let owner_map_digest =
                    laminar_core::checkpoint::CheckpointAssignmentFence::owner_map_digest_iter(
                        u32::from(self.key_group_count),
                        assignment.owners().iter().map(|owner| owner.0),
                    );
                let expected = (assignment.version(), owner_map_digest, config.self_id.0);
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
            if restored_cluster > restore_preflight.runtime_scratch {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] restored cluster state exceeds its preflighted bound",
                    self.projection.op_name
                )));
            }
            let restore_peak = self
                .accounted_state_bytes()
                .checked_add(restore_preflight.encoded_frame)
                .and_then(|bytes| bytes.checked_add(decoded_checkpoint_bytes))
                .and_then(|bytes| bytes.checked_add(restored_cluster))
                .ok_or_else(|| self.accounting_error())?;
            if restore_peak > self.max_managed_state_bytes {
                return Err(DbError::ManagedStateBudgetExceeded {
                    context: format!(
                        "interval join [{}] cluster checkpoint restore peak",
                        self.projection.op_name
                    ),
                    accounted_bytes: restore_peak,
                    limit_bytes: self.max_managed_state_bytes,
                });
            }
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
        if previous.watermark.is_some() && frontier.watermark.is_none() {
            self.validate_frontier(previous, frontier, side)?;
        }
        let floor = if side.port() == 0 {
            self.applied_left_watermark
        } else {
            self.applied_right_watermark
        };
        frontier.watermark = Self::max_watermark(frontier.watermark, Self::watermark_option(floor));
        self.validate_frontier(previous, frontier, side)?;
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
                let state_capture = state.capture_checkpoint(
                    &self.config,
                    usize::try_from(remaining_capture_bytes).unwrap_or(usize::MAX),
                )?;
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
            .and_then(|bytes| bytes.checked_sub(state.len()))
            .and_then(|bytes| bytes.checked_sub(vnode_checkpoint_alignment_copy_bytes(state)))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] live state plus encoded vnode and alignment copy exceed its {}-byte restore limit",
                    self.projection.op_name, self.max_managed_state_bytes
                ))
            })?;
        let ordered_cut = self
            .ordered_input_spec
            .as_ref()
            .map(|_| IntervalHandoffCut {
                left_watermark: self.applied_left_watermark,
                right_watermark: self.applied_right_watermark,
                #[cfg(feature = "cluster")]
                left_idle: self.applied_left_idle,
                #[cfg(feature = "cluster")]
                right_idle: self.applied_right_idle,
            });
        let replacement = Self::decode_vnode_frame(
            state,
            vnode,
            &self.config,
            self.ordered_input_spec.as_ref(),
            &format!(
                "interval join [{}] vnode {vnode} restore",
                self.projection.op_name
            ),
            remaining,
            self.max_managed_state_bytes,
            ordered_cut,
        )?;
        self.vnode_states[vnode as usize] = if let Some(mut replacement) = replacement {
            replacement.validate_vnode(vnode, vnode_count, &self.config)?;
            if replacement.ordered.is_none() {
                if let Some((left_schema, right_schema)) = &self.input_schemas {
                    replacement.seed_input_schemas(
                        left_schema.clone(),
                        right_schema.clone(),
                        &self.config,
                    )?;
                }
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
        let allocation = |bytes: usize| {
            bytes
                .checked_add(usize::from(bytes != 0) * HEAP_ALLOCATION_CHARGE)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] transition allocation accounting overflow",
                        self.projection.op_name
                    ))
                })
        };
        let roster = |capacity: usize, item_bytes: usize| {
            capacity
                .checked_mul(item_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] transition roster accounting overflow",
                        self.projection.op_name
                    ))
                })
                .and_then(allocation)
        };
        let live_bytes = self.accounted_state_bytes();
        let transition_payload_bytes = transition
            .restores
            .iter()
            .map(|restore| restore.state.len())
            .chain(
                transition
                    .whole_restores
                    .iter()
                    .map(|restore| restore.state.len()),
            )
            .try_fold(0usize, usize::checked_add)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] transition payload accounting overflow",
                    self.projection.op_name
                ))
            })?;
        let alignment_copy_bytes = transition
            .restores
            .iter()
            .map(|restore| vnode_checkpoint_alignment_copy_bytes(restore.state))
            .chain(
                transition
                    .whole_restores
                    .iter()
                    .map(|restore| checkpoint_alignment_copy_bytes(restore.state)),
            )
            .max()
            .unwrap_or(0);
        let replacement_capacity = transition
            .revoked
            .len()
            .checked_add(transition.restores.len())
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] transition replacement accounting overflow",
                    self.projection.op_name
                ))
            })?;
        let target_resident_capacity = self
            .resident_vnodes
            .len()
            .checked_add(transition.restores.len())
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] transition resident-roster accounting overflow",
                    self.projection.op_name
                ))
            })?;
        let target_peer_capacity = assignment.owners().len();
        let owner_roster_capacity = assignment
            .owners()
            .len()
            .max(self.local_assignment.owners().len());
        let owner_roster_charge = roster(owner_roster_capacity, std::mem::size_of::<u64>())?;
        let replacement_item_bytes =
            std::mem::size_of::<(u32, Option<Box<IntervalJoinVnodeState>>)>();
        let replacement_tree_payload = replacement_capacity
            .checked_mul(
                replacement_item_bytes
                    .checked_add(PEER_CHANNEL_ENTRY_CHARGE)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] transition replacement accounting overflow",
                            self.projection.op_name
                        ))
                    })?,
            )
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] transition replacement accounting overflow",
                    self.projection.op_name
                ))
            })?;
        let replacement_tree_charge = allocation(replacement_tree_payload)?;
        let peer_channel_payload = target_peer_capacity
            .checked_mul(2)
            .and_then(|entries| {
                entries.checked_mul(
                    std::mem::size_of::<(u64, IntervalPeerChannel)>()
                        .checked_add(PEER_CHANNEL_ENTRY_CHARGE)?,
                )
            })
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] transition channel accounting overflow",
                    self.projection.op_name
                ))
            })?;
        let peer_channel_charge = allocation(peer_channel_payload)?;
        let owner_peer_roster_charge = roster(owner_roster_capacity, std::mem::size_of::<u64>())?;
        let target_peer_roster_charge = roster(target_peer_capacity, std::mem::size_of::<u64>())?;
        let replacement_roster_charge = roster(replacement_capacity, replacement_item_bytes)?;
        let resident_roster_charge = roster(target_resident_capacity, std::mem::size_of::<u32>())?;
        let owner_tree_charge = allocation(
            owner_roster_capacity
                .checked_mul(
                    std::mem::size_of::<u64>()
                        .checked_add(PEER_CHANNEL_ENTRY_CHARGE)
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "interval join [{}] owner-tree accounting overflow",
                                self.projection.op_name
                            ))
                        })?,
                )
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] owner-tree accounting overflow",
                        self.projection.op_name
                    ))
                })?,
        )?;
        let donor_tree_charge = allocation(
            transition
                .restores
                .len()
                .checked_add(transition.whole_restores.len())
                .and_then(|entries| {
                    entries.checked_mul(
                        std::mem::size_of::<u64>().checked_add(PEER_CHANNEL_ENTRY_CHARGE)?,
                    )
                })
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] donor-tree accounting overflow",
                        self.projection.op_name
                    ))
                })?,
        )?;
        let mut scaffold_bytes = std::mem::size_of::<PreparedIntervalJoinTransition>();
        for charge in [
            std::mem::size_of::<VnodeAssignmentSnapshot>(),
            32,
            // Target, installed, optional bootstrap, and one internal owner-map roster may
            // overlap while assignment fences are validated.
            owner_roster_charge.checked_mul(4).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] transition scaffold accounting overflow",
                    self.projection.op_name
                ))
            })?,
            owner_tree_charge,
            donor_tree_charge,
            // Predecessor/target peer scratch and the final Arc allocation may overlap.
            owner_peer_roster_charge.checked_mul(2).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] transition scaffold accounting overflow",
                    self.projection.op_name
                ))
            })?,
            target_peer_roster_charge.checked_mul(2).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] transition scaffold accounting overflow",
                    self.projection.op_name
                ))
            })?,
            replacement_tree_charge,
            replacement_roster_charge,
            resident_roster_charge,
            peer_channel_charge,
            512,
        ] {
            scaffold_bytes = scaffold_bytes.checked_add(charge).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] transition scaffold accounting overflow",
                    self.projection.op_name
                ))
            })?;
        }
        let largest_whole_decode = transition
            .whole_restores
            .iter()
            .try_fold(0usize, |largest, restore| {
                restore
                    .state
                    .len()
                    .checked_mul(4)
                    .and_then(|bytes| bytes.checked_add(8 * HEAP_ALLOCATION_CHARGE))
                    .map(|bytes| largest.max(bytes))
            })
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] whole-checkpoint decode accounting overflow",
                    self.projection.op_name
                ))
            })?;
        let fixed_transition_bytes = live_bytes
            .checked_add(transition_payload_bytes)
            .and_then(|bytes| bytes.checked_add(alignment_copy_bytes))
            .and_then(|bytes| bytes.checked_add(scaffold_bytes))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] transition accounting overflow",
                    self.projection.op_name
                ))
            })?;
        let whole_decode_headroom = self
            .max_managed_state_bytes
            .checked_sub(fixed_transition_bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] transition live state, payload, alignment copy, and scaffolding exceed its {}-byte limit",
                    self.projection.op_name, self.max_managed_state_bytes
                ))
            })?;
        if largest_whole_decode > whole_decode_headroom {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] transition whole decode needs {largest_whole_decode} bytes; remaining headroom is {whole_decode_headroom} bytes",
                self.projection.op_name
            )));
        }

        let mut owners = Vec::new();
        owners
            .try_reserve_exact(assignment.owners().len())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "interval join [{}] target owner-roster reservation failed: {error}",
                    self.projection.op_name
                ))
            })?;
        owners.extend(assignment.owners().iter().map(|owner| owner.0));
        let mut installed_owners = Vec::new();
        installed_owners
            .try_reserve_exact(self.local_assignment.owners().len())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "interval join [{}] installed owner-roster reservation failed: {error}",
                    self.projection.op_name
                ))
            })?;
        installed_owners.extend(self.local_assignment.owners().iter().map(|owner| owner.0));
        let checkpoint_bootstrap = match transition.mode {
            ManagedVnodeTransitionMode::Live => false,
            ManagedVnodeTransitionMode::CheckpointBootstrap { predecessor_owners } => {
                let mut predecessor_owner_ids = Vec::new();
                predecessor_owner_ids
                    .try_reserve_exact(predecessor_owners.len())
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] predecessor owner-roster reservation failed: {error}",
                            self.projection.op_name
                        ))
                    })?;
                predecessor_owner_ids.extend(predecessor_owners.iter().map(|owner| owner.0));
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
        let version_edge_valid = if checkpoint_bootstrap {
            transition.predecessor.assignment_version < transition.target.assignment_version
        } else {
            transition.predecessor.assignment_version.checked_add(1)
                == Some(transition.target.assignment_version)
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
            || !version_edge_valid
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
        let predecessor_peers = Self::try_remote_owner_peers(
            &self.local_assignment,
            config.self_id,
            "interval join transition predecessor",
        )?;
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
                .filter_map(|(owner, vnode)| (*owner == config.self_id).then_some(vnode));
            let target_owned_count = target_owned.clone().count();
            let exact_restore_roster = transition
                .restores
                .iter()
                .map(|restore| restore.vnode)
                .eq(target_owned);
            if !transition.revoked.is_empty() || target_owned_count == 0 || !exact_restore_roster {
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
        let mut handoff_cut =
            self.portable_handoff_cut(&transition, requires_handoff_cut, whole_decode_headroom)?;
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
        for (restore_index, restore) in transition.restores.iter().enumerate() {
            if restore_index != 0 && transition.restores[restore_index - 1].vnode >= restore.vnode {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] transition restore roster is not strictly ordered at vnode {}",
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
        }

        let mut restored_bytes = 0usize;
        let mut recovered_cut: Option<IntervalHandoffCut> = None;
        for restore in transition.restores {
            let remaining = self
                .max_managed_state_bytes
                .checked_sub(fixed_transition_bytes)
                .and_then(|bytes| bytes.checked_sub(restored_bytes))
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
                restore.vnode,
                &self.config,
                self.ordered_input_spec.as_ref(),
                &context,
                remaining,
                self.max_managed_state_bytes,
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
            if state.ordered.is_none() {
                if let Some((left_schema, right_schema)) = &self.input_schemas {
                    state.seed_input_schemas(
                        left_schema.clone(),
                        right_schema.clone(),
                        &self.config,
                    )?;
                }
            }
            state.validate_vnode(restore.vnode, transition.target.vnode_count, &self.config)?;
            restored_bytes = restored_bytes
                .checked_add(state.accounted_state_bytes())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] restored vnode accounting overflow",
                        self.projection.op_name
                    ))
                })?;
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
                let context = format!(
                    "interval join [{}] vnode {vnode} restore",
                    self.projection.op_name
                );
                if state.ordered.is_some() {
                    Self::validate_ordered_core_cutoffs(state, &self.config, &context, cut)?;
                } else {
                    let (left_rows, right_rows) = state.buffered_rows();
                    let (left_evicted_cutoff, right_evicted_cutoff) = state.evicted_cutoffs();
                    Self::validate_handoff_cutoffs(
                        left_evicted_cutoff,
                        right_evicted_cutoff,
                        left_rows != 0,
                        right_rows != 0,
                        &self.config,
                        &context,
                        cut,
                    )?;
                }
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
        let target_peers = Self::try_remote_owner_peers(
            &assignment,
            config.self_id,
            "interval join transition target",
        )?;
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
        let mut replacement_roster = Vec::new();
        replacement_roster
            .try_reserve_exact(replacement_capacity)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "interval join [{}] replacement-roster reservation failed: {error}",
                    self.projection.op_name
                ))
            })?;
        replacement_roster.extend(replacements);
        let prepared = PreparedIntervalJoinTransition {
            replacements: replacement_roster,
            local_assignment: assignment,
            resident_vnodes,
            cluster_peers: target_peers.into(),
            peer_channels,
            bootstrap_broadcast,
            handoff_cut,
        };
        let total_bytes = live_bytes
            .checked_add(transition_payload_bytes)
            .and_then(|bytes| bytes.checked_add(Self::transition_accounted_bytes(&prepared)))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] prepared transition accounting overflow",
                    self.projection.op_name
                ))
            })?;
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
mod tests;
