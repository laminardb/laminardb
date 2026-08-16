//! Managed vnode-local temporal join execution.

use std::collections::BTreeMap;
#[cfg(feature = "cluster")]
use std::collections::VecDeque;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
#[cfg(feature = "cluster")]
use arrow::ipc::reader::StreamReader;
use arrow::row::Rows;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use laminar_connectors::connector::{
    source_mutations, source_mutations_routed, strip_source_mutations,
    strip_source_mutations_routed,
};
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::CheckpointAssignmentFence;
use laminar_core::state::{
    KeyGroupCount, NodeId, PartitionKeyCodecV1, VnodeAssignmentSnapshot, VnodeRegistry,
    LOCAL_NODE_ID,
};
use laminar_sql::temporal::{MAX_TEMPORAL_PROBES_PER_ROW, MAX_TEMPORAL_PROBE_HORIZON_MS};
use laminar_sql::translator::TemporalJoinTranslatorConfig;

use crate::error::DbError;
use crate::operator::capability::OperatorCapability;
#[cfg(feature = "cluster")]
use crate::operator::sql_query::ClusterShuffleConfig;
use crate::operator::ProjectingJoinState;
#[cfg(feature = "cluster")]
use crate::operator_graph::merge_input_frontier_iter;
use crate::operator_graph::{
    merge_input_frontiers, CapturedVnodeState, EncodedStateFrame, GraphOperator, InputFrontier,
    ManagedStateAccountingSnapshot, OperatorCheckpoint, StateFrameCapture,
};
#[cfg(feature = "cluster")]
use crate::operator_graph::{ManagedVnodeTransition, ManagedVnodeTransitionMode};
use crate::temporal_join_state::{
    temporal_join_output_schema, TemporalJoinStateConfig, TemporalJoinVnodeState,
    TemporalStateLimits, TimestampMillisView,
};

const ABSENT_VNODE: u8 = 0;
const PRESENT_VNODE: u8 = 1;
const OPERATOR_CHECKPOINT_VERSION: u8 = 2;
const OPERATOR_CAPTURE_ALLOCATION_CHARGE: usize = 32;
const OPERATOR_CHECKPOINT_BASE_SCRATCH: usize = 512;
const CHECKPOINT_ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;
const PENDING_HOLD_ENTRY_CHARGE: usize = 64;
const TEMPORAL_TMP_TABLE: &str = "__temporal_tmp";
const MAX_PENDING_PROBES_PER_VNODE: usize = 1_000_000;
const READY_PROBE_BUDGET: usize = 1_024;
const HISTORY_GC_BUDGET: usize = 1_024;
const MAINTENANCE_VNODE_BUDGET: usize = 64;
#[cfg(feature = "cluster")]
const REMOTE_EVENT_BUDGET_PER_SIDE: usize = 64;
#[cfg(feature = "cluster")]
const REMOTE_EVENT_CHARGE: usize = std::mem::size_of::<TemporalRemoteEvent>();
#[cfg(feature = "cluster")]
const RETAINED_BATCH_ARC_CHARGE: usize =
    std::mem::size_of::<crate::operator::RetainedBatch>() + 2 * std::mem::size_of::<usize>();
#[cfg(feature = "cluster")]
const REMOTE_DRAIN_BYTE_BUDGET_PER_SIDE: usize = laminar_core::shuffle::ROUTE_MAX_BATCH_BYTES * 2;
#[cfg(feature = "cluster")]
const REMOTE_DRAIN_ROW_BUDGET_PER_SIDE: usize = laminar_core::shuffle::ROUTE_MAX_BATCH_ROWS * 2;
#[cfg(feature = "cluster")]
const WHOLE_RESTORE_IPC_EXPANSION_FACTOR: usize = 8;
#[cfg(feature = "cluster")]
const WHOLE_RESTORE_ROW_SCRATCH_CHARGE: usize = 2_048;

fn checkpoint_allocation_bytes(payload: usize) -> Result<usize, DbError> {
    payload
        .checked_add(usize::from(payload != 0) * OPERATOR_CAPTURE_ALLOCATION_CHARGE)
        .ok_or_else(|| {
            DbError::Checkpoint("temporal whole-checkpoint allocation accounting overflow".into())
        })
}

fn checkpoint_roster_bytes(count: usize, item_bytes: usize) -> Result<usize, DbError> {
    count
        .checked_mul(item_bytes)
        .ok_or_else(|| {
            DbError::Checkpoint("temporal whole-checkpoint roster accounting overflow".into())
        })
        .and_then(checkpoint_allocation_bytes)
}

fn checkpoint_alignment_copy_bytes(bytes: &[u8]) -> usize {
    if bytes.as_ptr().align_offset(CHECKPOINT_ARCHIVE_ALIGNMENT) == 0 {
        0
    } else {
        bytes.len()
    }
}

fn checkpoint_alignment_copy_charge(bytes: &[u8]) -> Result<usize, DbError> {
    checkpoint_allocation_bytes(checkpoint_alignment_copy_bytes(bytes))
}

fn vnode_checkpoint_alignment_copy_bytes(bytes: &[u8]) -> usize {
    bytes.get(1..).map_or(0, checkpoint_alignment_copy_bytes)
}

fn vnode_checkpoint_alignment_copy_charge(bytes: &[u8]) -> Result<usize, DbError> {
    checkpoint_allocation_bytes(vnode_checkpoint_alignment_copy_bytes(bytes))
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

#[cfg(feature = "cluster")]
#[derive(Clone, Copy)]
struct TemporalIpcRestorePreflight {
    rows: usize,
    scratch_rows: usize,
    body_bytes: usize,
}

#[cfg(feature = "cluster")]
fn preflight_temporal_channel_ipc_restore(
    bytes: &[u8],
    expected_rows: impl IntoIterator<Item = u64>,
    context: &str,
) -> Result<TemporalIpcRestorePreflight, DbError> {
    const CONTINUATION: u32 = u32::MAX;

    let mut expected_rows = expected_rows.into_iter();
    let Some(first_expected) = expected_rows.next() else {
        if bytes.is_empty() {
            return Ok(TemporalIpcRestorePreflight {
                rows: 0,
                scratch_rows: 0,
                body_bytes: 0,
            });
        }
        return Err(DbError::Checkpoint(format!(
            "{context} IPC exists without queued data events"
        )));
    };
    let mut next_expected = Some(first_expected);
    let mut offset = 0usize;
    let mut saw_schema = false;
    let mut rows = 0usize;
    let mut scratch_rows = 0usize;
    let mut body_bytes = 0usize;
    loop {
        let prefix_end = offset
            .checked_add(4)
            .ok_or_else(|| DbError::Checkpoint(format!("{context} IPC framing overflow")))?;
        let prefix = bytes
            .get(offset..prefix_end)
            .ok_or_else(|| DbError::Checkpoint(format!("{context} IPC frame is truncated")))?;
        offset = prefix_end;
        let mut metadata_len = u32::from_le_bytes(prefix.try_into().expect("four-byte prefix"));
        if metadata_len == CONTINUATION {
            let length_end = offset
                .checked_add(4)
                .ok_or_else(|| DbError::Checkpoint(format!("{context} IPC framing overflow")))?;
            let length = bytes.get(offset..length_end).ok_or_else(|| {
                DbError::Checkpoint(format!("{context} IPC continuation is truncated"))
            })?;
            offset = length_end;
            metadata_len = u32::from_le_bytes(length.try_into().expect("four-byte length"));
        }
        if metadata_len == 0 {
            if offset != bytes.len() || !saw_schema || next_expected.is_some() {
                return Err(DbError::Checkpoint(format!(
                    "{context} IPC stream is non-canonical"
                )));
            }
            break;
        }
        let metadata_len = usize::try_from(metadata_len).map_err(|_| {
            DbError::Checkpoint(format!("{context} IPC metadata length exceeds usize"))
        })?;
        let metadata_end = offset.checked_add(metadata_len).ok_or_else(|| {
            DbError::Checkpoint(format!("{context} IPC metadata framing overflow"))
        })?;
        let metadata = bytes
            .get(offset..metadata_end)
            .ok_or_else(|| DbError::Checkpoint(format!("{context} IPC metadata is truncated")))?;
        offset = metadata_end;
        let message = arrow_ipc::root_as_message(metadata).map_err(|error| {
            DbError::Checkpoint(format!("{context} IPC metadata is invalid: {error}"))
        })?;
        let body_len = usize::try_from(message.bodyLength()).map_err(|_| {
            DbError::Checkpoint(format!(
                "{context} IPC body length is negative or too large"
            ))
        })?;
        let body_end = offset
            .checked_add(body_len)
            .ok_or_else(|| DbError::Checkpoint(format!("{context} IPC body framing overflow")))?;
        if body_end > bytes.len() {
            return Err(DbError::Checkpoint(format!(
                "{context} IPC body is truncated"
            )));
        }
        body_bytes = body_bytes.checked_add(body_len).ok_or_else(|| {
            DbError::Checkpoint(format!("{context} IPC body accounting overflow"))
        })?;
        match message.header_type() {
            arrow_ipc::MessageHeader::Schema if !saw_schema && rows == 0 => {
                saw_schema = true;
            }
            arrow_ipc::MessageHeader::DictionaryBatch if saw_schema => {
                let dictionary = message.header_as_dictionary_batch().ok_or_else(|| {
                    DbError::Checkpoint(format!("{context} IPC dictionary-batch header is missing"))
                })?;
                let data = dictionary.data().ok_or_else(|| {
                    DbError::Checkpoint(format!("{context} IPC dictionary-batch data is missing"))
                })?;
                let dictionary_rows = usize::try_from(data.length()).map_err(|_| {
                    DbError::Checkpoint(format!(
                        "{context} IPC dictionary-batch length is negative or too large"
                    ))
                })?;
                scratch_rows = scratch_rows.checked_add(dictionary_rows).ok_or_else(|| {
                    DbError::Checkpoint(format!("{context} IPC dictionary row accounting overflow"))
                })?;
                if data.compression().is_some() {
                    return Err(DbError::Checkpoint(format!(
                        "{context} IPC compression is unsupported"
                    )));
                }
            }
            arrow_ipc::MessageHeader::RecordBatch if saw_schema => {
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    DbError::Checkpoint(format!("{context} IPC record-batch header is missing"))
                })?;
                if batch.compression().is_some() {
                    return Err(DbError::Checkpoint(format!(
                        "{context} IPC compression is unsupported"
                    )));
                }
                let batch_rows = usize::try_from(batch.length()).map_err(|_| {
                    DbError::Checkpoint(format!(
                        "{context} IPC record-batch length is negative or too large"
                    ))
                })?;
                let expected = next_expected.take().ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "{context} IPC has more batches than queued data events"
                    ))
                })?;
                if u64::try_from(batch_rows).ok() != Some(expected) {
                    return Err(DbError::Checkpoint(format!(
                        "{context} IPC row count does not match its queued data event"
                    )));
                }
                rows = rows.checked_add(batch_rows).ok_or_else(|| {
                    DbError::Checkpoint(format!("{context} IPC row accounting overflow"))
                })?;
                scratch_rows = scratch_rows.checked_add(batch_rows).ok_or_else(|| {
                    DbError::Checkpoint(format!("{context} IPC row accounting overflow"))
                })?;
                next_expected = expected_rows.next();
            }
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "{context} IPC message order is non-canonical"
                )));
            }
        }
        offset = body_end;
    }
    Ok(TemporalIpcRestorePreflight {
        rows,
        scratch_rows,
        body_bytes,
    })
}

#[derive(Clone, Copy, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct TemporalCheckpointFrontier {
    watermark: Option<i64>,
    idle: bool,
}

impl From<InputFrontier> for TemporalCheckpointFrontier {
    fn from(frontier: InputFrontier) -> Self {
        Self {
            watermark: frontier.watermark,
            idle: frontier.idle,
        }
    }
}

impl From<TemporalCheckpointFrontier> for InputFrontier {
    fn from(frontier: TemporalCheckpointFrontier) -> Self {
        Self {
            watermark: frontier.watermark,
            idle: frontier.idle,
        }
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
enum TemporalCheckpointEvent {
    Data {
        recovery_gen: u64,
        routed_vnodes: Vec<u32>,
        row_count: u64,
        mutation_stream: bool,
    },
    Frontier {
        recovery_gen: u64,
        frontier: TemporalCheckpointFrontier,
    },
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct TemporalCheckpointChannel {
    peer: u64,
    applied: TemporalCheckpointFrontier,
    events: Vec<TemporalCheckpointEvent>,
    positioned_ipc: Vec<u8>,
    mutation_ipc: Vec<u8>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct TemporalClusterCheckpoint {
    assignment_version: u64,
    owner_map_digest: [u8; 32],
    self_id: u64,
    local_frontiers: [TemporalCheckpointFrontier; 2],
    remote_peer_cursors: [Option<u64>; 2],
    channels: [Vec<TemporalCheckpointChannel>; 2],
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct TemporalJoinOperatorCheckpoint {
    version: u8,
    frontiers: [TemporalCheckpointFrontier; 2],
    maintenance_cursor: u32,
    maintenance_pending: bool,
    maintenance_remaining: u32,
    maintenance_rescan: bool,
    published_output_frontier: Option<TemporalCheckpointFrontier>,
    cluster: Option<TemporalClusterCheckpoint>,
}

#[cfg(feature = "cluster")]
enum CapturedTemporalCheckpointEvent {
    Data {
        recovery_gen: u64,
        retained: Arc<crate::operator::RetainedBatch>,
        mutation_stream: bool,
    },
    Frontier {
        recovery_gen: u64,
        frontier: InputFrontier,
    },
}

#[cfg(feature = "cluster")]
struct CapturedTemporalCheckpointChannel {
    peer: u64,
    applied: InputFrontier,
    events: Vec<CapturedTemporalCheckpointEvent>,
}

#[cfg(feature = "cluster")]
struct CapturedTemporalClusterCheckpoint {
    assignment_version: u64,
    owner_map_digest: [u8; 32],
    self_id: u64,
    local_frontiers: [InputFrontier; 2],
    remote_peer_cursors: [Option<u64>; 2],
    channels: [Vec<CapturedTemporalCheckpointChannel>; 2],
    retained_bytes: usize,
}

struct TemporalJoinOperatorCheckpointCapture {
    checkpoint: TemporalJoinOperatorCheckpoint,
    #[cfg(feature = "cluster")]
    cluster: Option<CapturedTemporalClusterCheckpoint>,
    retained_bytes: usize,
}

struct TemporalWholeRestorePreflight {
    decoded_checkpoint: usize,
    runtime_scratch: usize,
}

impl TemporalJoinOperatorCheckpointCapture {
    const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn encode(self, max_encoded_bytes: usize) -> Result<Vec<u8>, DbError> {
        #[cfg(feature = "cluster")]
        let mut capture = self;
        #[cfg(not(feature = "cluster"))]
        let capture = self;
        let remaining = max_encoded_bytes
            .checked_sub(OPERATOR_CHECKPOINT_BASE_SCRATCH)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "temporal operator checkpoint scratch requires {OPERATOR_CHECKPOINT_BASE_SCRATCH} bytes; encoding headroom is {max_encoded_bytes} bytes"
                ))
            })?;
        #[cfg(feature = "cluster")]
        let remaining = if let Some(cluster) = capture.cluster.take() {
            let (encoded, remaining) = cluster.encode(remaining)?;
            capture.checkpoint.cluster = Some(encoded);
            remaining
        } else {
            remaining
        };
        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(remaining),
        );
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&capture.checkpoint, writer)
            .map(|bytes| bytes.into_inner().into_vec())
            .map_err(|error| {
                DbError::Checkpoint(format!("temporal join operator checkpoint: {error}"))
            })
    }
}

#[cfg(feature = "cluster")]
impl CapturedTemporalClusterCheckpoint {
    const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn encoding_scratch_bytes(&self) -> Result<usize, DbError> {
        let mut bytes = 0usize;
        let mut add = |amount: usize| -> Result<(), DbError> {
            bytes = bytes.checked_add(amount).ok_or_else(|| {
                DbError::Checkpoint("temporal channel checkpoint scratch overflow".into())
            })?;
            Ok(())
        };
        for channels in &self.channels {
            add(channels
                .len()
                .checked_mul(std::mem::size_of::<TemporalCheckpointChannel>())
                .and_then(|value| {
                    value.checked_add(usize::from(value != 0) * OPERATOR_CAPTURE_ALLOCATION_CHARGE)
                })
                .ok_or_else(|| {
                    DbError::Checkpoint("temporal channel checkpoint scratch overflow".into())
                })?)?;
            for channel in channels {
                add(channel
                    .events
                    .len()
                    .checked_mul(std::mem::size_of::<TemporalCheckpointEvent>())
                    .and_then(|value| {
                        value.checked_add(
                            usize::from(value != 0) * OPERATOR_CAPTURE_ALLOCATION_CHARGE,
                        )
                    })
                    .ok_or_else(|| {
                        DbError::Checkpoint("temporal channel checkpoint scratch overflow".into())
                    })?)?;
                for event in &channel.events {
                    if let CapturedTemporalCheckpointEvent::Data { retained, .. } = event {
                        add(retained
                            .routed_vnodes()
                            .len()
                            .checked_mul(std::mem::size_of::<u32>())
                            .and_then(|value| {
                                value.checked_add(
                                    usize::from(value != 0) * OPERATOR_CAPTURE_ALLOCATION_CHARGE,
                                )
                            })
                            .ok_or_else(|| {
                                DbError::Checkpoint(
                                    "temporal channel checkpoint scratch overflow".into(),
                                )
                            })?)?;
                        add(retained.heap_bytes().ok_or_else(|| {
                            DbError::Checkpoint(
                                "temporal channel checkpoint scratch overflow".into(),
                            )
                        })?)?;
                    }
                }
            }
        }
        Ok(bytes)
    }

    fn encode(
        self,
        max_encoded_bytes: usize,
    ) -> Result<(TemporalClusterCheckpoint, usize), DbError> {
        let scratch_bytes = self.encoding_scratch_bytes()?;
        let mut remaining = max_encoded_bytes.checked_sub(scratch_bytes).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "temporal channel checkpoint scratch requires {scratch_bytes} bytes; encoding headroom is {max_encoded_bytes} bytes"
            ))
        })?;
        let mut encoded_channels = [
            Vec::with_capacity(self.channels[0].len()),
            Vec::with_capacity(self.channels[1].len()),
        ];
        for (port, channels) in self.channels.into_iter().enumerate() {
            for channel in channels {
                let encode_stream =
                    |mutation_stream: bool, remaining: &mut usize| -> Result<Vec<u8>, DbError> {
                        let first_batch = channel.events.iter().find_map(|event| match event {
                            CapturedTemporalCheckpointEvent::Data {
                                retained,
                                mutation_stream: event_stream,
                                ..
                            } if *event_stream == mutation_stream => Some(retained.batch()),
                            CapturedTemporalCheckpointEvent::Data { .. }
                            | CapturedTemporalCheckpointEvent::Frontier { .. } => None,
                        });
                        let Some(first_batch) = first_batch else {
                            return Ok(Vec::new());
                        };
                        let batches = channel.events.iter().filter_map(|event| match event {
                            CapturedTemporalCheckpointEvent::Data {
                                retained,
                                mutation_stream: event_stream,
                                ..
                            } if *event_stream == mutation_stream => Some(retained.batch()),
                            CapturedTemporalCheckpointEvent::Data { .. }
                            | CapturedTemporalCheckpointEvent::Frontier { .. } => None,
                        });
                        let ipc = laminar_core::serialization::serialize_batches_stream_bounded(
                            first_batch.schema().as_ref(),
                            batches,
                            *remaining,
                        )
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "temporal {} peer {} {} channel IPC: {error}",
                                if port == 0 { "left" } else { "right" },
                                channel.peer,
                                if mutation_stream {
                                    "mutation"
                                } else {
                                    "positioned"
                                }
                            ))
                        })?;
                        *remaining = remaining.checked_sub(ipc.capacity()).ok_or_else(|| {
                            DbError::Checkpoint(
                                "temporal channel IPC exceeded its encoding budget".into(),
                            )
                        })?;
                        Ok(ipc)
                    };
                let positioned_ipc = encode_stream(false, &mut remaining)?;
                let mutation_ipc = encode_stream(true, &mut remaining)?;
                let events = channel
                    .events
                    .into_iter()
                    .map(|event| match event {
                        CapturedTemporalCheckpointEvent::Data {
                            recovery_gen,
                            retained,
                            mutation_stream,
                        } => Ok(TemporalCheckpointEvent::Data {
                            recovery_gen,
                            routed_vnodes: retained.routed_vnodes().to_vec(),
                            row_count: u64::try_from(retained.batch().num_rows()).map_err(
                                |_| {
                                    DbError::Checkpoint(
                                        "temporal channel row count exceeds u64".into(),
                                    )
                                },
                            )?,
                            mutation_stream,
                        }),
                        CapturedTemporalCheckpointEvent::Frontier {
                            recovery_gen,
                            frontier,
                        } => Ok(TemporalCheckpointEvent::Frontier {
                            recovery_gen,
                            frontier: frontier.into(),
                        }),
                    })
                    .collect::<Result<Vec<_>, DbError>>()?;
                encoded_channels[port].push(TemporalCheckpointChannel {
                    peer: channel.peer,
                    applied: channel.applied.into(),
                    events,
                    positioned_ipc,
                    mutation_ipc,
                });
            }
        }
        Ok((
            TemporalClusterCheckpoint {
                assignment_version: self.assignment_version,
                owner_map_digest: self.owner_map_digest,
                self_id: self.self_id,
                local_frontiers: self.local_frontiers.map(Into::into),
                remote_peer_cursors: self.remote_peer_cursors,
                channels: encoded_channels,
            },
            remaining,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TemporalJoinExecutionLimits {
    pub(crate) left_allowed_lateness_ms: i64,
    pub(crate) right_allowed_lateness_ms: i64,
    pub(crate) history_retention_ms: i64,
    pub(crate) max_pending_probes: usize,
    pub(crate) ready_probe_budget: NonZeroUsize,
    pub(crate) history_gc_budget: NonZeroUsize,
    pub(crate) maintenance_vnode_budget: NonZeroUsize,
}

impl TemporalJoinExecutionLimits {
    pub(crate) fn production(history_retention_ms: i64) -> Self {
        debug_assert!(history_retention_ms > 0);
        Self {
            // Source watermarks already include configured out-of-orderness. Adding another
            // allowance here would both admit rows behind that contract and delay final output.
            left_allowed_lateness_ms: 0,
            right_allowed_lateness_ms: 0,
            history_retention_ms,
            max_pending_probes: MAX_PENDING_PROBES_PER_VNODE,
            ready_probe_budget: NonZeroUsize::new(READY_PROBE_BUDGET).expect("positive constant"),
            history_gc_budget: NonZeroUsize::new(HISTORY_GC_BUDGET).expect("positive constant"),
            maintenance_vnode_budget: NonZeroUsize::new(MAINTENANCE_VNODE_BUDGET)
                .expect("positive constant"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemporalInputSide {
    Left,
    Right,
}

impl TemporalInputSide {
    #[cfg(feature = "cluster")]
    const fn port(self) -> usize {
        match self {
            Self::Left => 0,
            Self::Right => 1,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Left => "left",
            Self::Right => "right",
        }
    }
}

#[derive(Clone)]
struct RoutedTemporalBatch {
    batch: RecordBatch,
    keys: Arc<Rows>,
    source_rows: Arc<[u32]>,
}

#[cfg(feature = "cluster")]
struct QueuedTemporalBatch {
    retained: Arc<crate::operator::RetainedBatch>,
    keys: Arc<Rows>,
    row_vnodes: Vec<u32>,
    charged_bytes: usize,
    mutation_stream: bool,
}

#[cfg(feature = "cluster")]
struct TemporalRemoteEvent {
    assignment_version: u64,
    recovery_gen: u64,
    payload: TemporalRemoteEventPayload,
}

#[cfg(feature = "cluster")]
enum TemporalRemoteEventPayload {
    Data(QueuedTemporalBatch),
    Frontier(InputFrontier),
}

#[cfg(feature = "cluster")]
impl TemporalRemoteEvent {
    fn payload_bytes(&self) -> usize {
        match &self.payload {
            TemporalRemoteEventPayload::Data(batch) => batch.charged_bytes,
            TemporalRemoteEventPayload::Frontier(_) => 0,
        }
    }

    fn rows(&self) -> usize {
        match &self.payload {
            TemporalRemoteEventPayload::Data(batch) => batch.retained.batch().num_rows(),
            TemporalRemoteEventPayload::Frontier(_) => 0,
        }
    }

    fn drain_bytes(&self) -> Option<usize> {
        self.payload_bytes()
            .checked_add(REMOTE_EVENT_CHARGE)?
            .checked_add(match &self.payload {
                TemporalRemoteEventPayload::Data(batch) => {
                    batch.retained.batch().get_array_memory_size()
                }
                TemporalRemoteEventPayload::Frontier(_) => 0,
            })
    }
}

#[cfg(feature = "cluster")]
#[derive(Default)]
struct TemporalPeerChannel {
    applied: InputFrontier,
    accepted: InputFrontier,
    events: VecDeque<TemporalRemoteEvent>,
}

#[cfg(feature = "cluster")]
struct TemporalVnodeInventory {
    resident_vnodes: Vec<u32>,
    vnode_pending_holds: Vec<Option<i64>>,
    pending_hold_counts: BTreeMap<i64, usize>,
    retained_state_bytes: usize,
    maintenance_pending: bool,
}

#[cfg(feature = "cluster")]
#[derive(Clone, Copy)]
struct TemporalHandoffCut {
    frontiers: [InputFrontier; 2],
    published_output_frontier: Option<InputFrontier>,
}

#[cfg(feature = "cluster")]
struct PreparedTemporalJoinTransition {
    slots: Vec<(u32, Option<Box<TemporalJoinVnodeState>>)>,
    local_assignment: VnodeAssignmentSnapshot,
    resident_vnodes: Vec<u32>,
    vnode_pending_holds: Vec<Option<i64>>,
    pending_hold_counts: BTreeMap<i64, usize>,
    retained_state_bytes: usize,
    maintenance_pending: bool,
    cluster_peers: Arc<[u64]>,
    peer_channels: [BTreeMap<u64, TemporalPeerChannel>; 2],
    bootstrap_broadcast: bool,
    handoff_cut: Option<TemporalHandoffCut>,
}

#[cfg(feature = "cluster")]
enum TemporalJoinTransitionCleanup {
    Aborted(PreparedTemporalJoinTransition),
    Published(PreparedTemporalJoinTransition),
}

#[cfg(feature = "cluster")]
struct DecodedTemporalCluster {
    local_frontiers: [InputFrontier; 2],
    peer_channels: [BTreeMap<u64, TemporalPeerChannel>; 2],
    remote_peer_cursors: [Option<u64>; 2],
    queued_shuffle_bytes: usize,
    queued_remote_events: usize,
    queued_event_capacity_bytes: usize,
}

#[cfg(feature = "cluster")]
struct RemoteDrainPlan {
    routed: BTreeMap<u32, [Vec<RoutedTemporalBatch>; 2]>,
    applied: [BTreeMap<u64, InputFrontier>; 2],
    consumed: [BTreeMap<u64, usize>; 2],
    cursors: [Option<u64>; 2],
    released_bytes: usize,
}

#[cfg(feature = "cluster")]
struct ClusterInputPlan {
    routed: BTreeMap<u32, [Vec<RoutedTemporalBatch>; 2]>,
    outbound: Vec<(u64, laminar_core::shuffle::ShuffleMessage)>,
    local_frontiers: [InputFrontier; 2],
    effective_frontiers: [InputFrontier; 2],
}

#[cfg(feature = "cluster")]
type TemporalSendTask = tokio::task::JoinHandle<(
    Result<(), DbError>,
    Option<Vec<(u64, laminar_core::shuffle::ShuffleMessage)>>,
)>;

#[cfg(feature = "cluster")]
struct PendingTemporalClusterInput {
    routed: BTreeMap<u32, [Vec<RoutedTemporalBatch>; 2]>,
    outbound: Option<Vec<(u64, laminar_core::shuffle::ShuffleMessage)>>,
    local_frontiers: [InputFrontier; 2],
    send: Option<TemporalSendTask>,
    accounted_bytes: usize,
}

#[cfg(feature = "cluster")]
enum PendingTemporalCompletion {
    Waiting,
    RetryLater,
    Applied(Vec<RecordBatch>),
}

#[cfg(feature = "cluster")]
impl Drop for PendingTemporalClusterInput {
    fn drop(&mut self) {
        if let Some(send) = &self.send {
            send.abort();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WholeRestoreState {
    Pending,
    Applied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VnodeCaptureMode {
    Full,
    Sparse,
}

pub(crate) struct ManagedTemporalJoinOperator {
    name: Arc<str>,
    projection: ProjectingJoinState,
    config: TemporalJoinTranslatorConfig,
    limits: TemporalJoinExecutionLimits,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    projection_input_schema: SchemaRef,
    left_key_indices: Vec<usize>,
    right_key_indices: Vec<usize>,
    key_codec: Arc<PartitionKeyCodecV1>,
    vnode_count: NonZeroU32,
    left_time_index: usize,
    right_time_index: usize,
    minimum_probe_offset: i64,
    key_group_count: KeyGroupCount,
    local_assignment: VnodeAssignmentSnapshot,
    vnode_states: Vec<Option<Box<TemporalJoinVnodeState>>>,
    resident_vnodes: Vec<u32>,
    vnode_pending_holds: Vec<Option<i64>>,
    pending_hold_counts: BTreeMap<i64, usize>,
    dirty_vnodes: Vec<bool>,
    dirty_vnode_roster: Vec<u32>,
    vnode_capture_mode: VnodeCaptureMode,
    retained_state_bytes: usize,
    max_managed_state_bytes: usize,
    frontiers: [InputFrontier; 2],
    restored_frontiers: Option<[InputFrontier; 2]>,
    whole_restore: WholeRestoreState,
    pending_frontiers: Option<[InputFrontier; 2]>,
    frontier_cursor: usize,
    frontier_remaining: usize,
    frontier_has_work: bool,
    maintenance_cursor: usize,
    maintenance_pending: bool,
    maintenance_remaining: usize,
    maintenance_rescan: bool,
    published_output_frontier: Option<InputFrontier>,
    #[cfg(feature = "cluster")]
    left_stage: String,
    #[cfg(feature = "cluster")]
    right_stage: String,
    #[cfg(feature = "cluster")]
    cluster_shuffle: Option<ClusterShuffleConfig>,
    #[cfg(feature = "cluster")]
    cluster_peers: Arc<[u64]>,
    #[cfg(feature = "cluster")]
    local_frontiers: [InputFrontier; 2],
    #[cfg(feature = "cluster")]
    peer_channels: [BTreeMap<u64, TemporalPeerChannel>; 2],
    #[cfg(feature = "cluster")]
    last_broadcasts: [InputFrontier; 2],
    #[cfg(feature = "cluster")]
    remote_peer_cursors: [Option<u64>; 2],
    #[cfg(feature = "cluster")]
    queued_shuffle_bytes: usize,
    #[cfg(feature = "cluster")]
    queued_remote_events: usize,
    #[cfg(feature = "cluster")]
    queued_event_capacity_bytes: usize,
    #[cfg(feature = "cluster")]
    pending_cluster_input: Option<PendingTemporalClusterInput>,
    #[cfg(feature = "cluster")]
    prepared_vnode_transition: Option<PreparedTemporalJoinTransition>,
    #[cfg(feature = "cluster")]
    vnode_transition_cleanup: Option<TemporalJoinTransitionCleanup>,
}

impl ManagedTemporalJoinOperator {
    pub(crate) fn try_new(
        name: &str,
        config: TemporalJoinTranslatorConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        key_group_count: KeyGroupCount,
        limits: TemporalJoinExecutionLimits,
    ) -> Result<Self, DbError> {
        if config.left_key_columns.is_empty()
            || config.left_key_columns.len() != config.right_key_columns.len()
        {
            return Err(DbError::Config(format!(
                "temporal join [{name}] requires paired equality keys"
            )));
        }
        let left_key_indices = config
            .left_key_columns
            .iter()
            .map(|column| column_index(&left_schema, column, name, "left key"))
            .collect::<Result<Vec<_>, _>>()?;
        let right_key_indices = config
            .right_key_columns
            .iter()
            .map(|column| column_index(&right_schema, column, name, "right key"))
            .collect::<Result<Vec<_>, _>>()?;
        let key_codec = Arc::new(
            PartitionKeyCodecV1::try_new(
                left_key_indices
                    .iter()
                    .map(|&index| left_schema.field(index).data_type().clone()),
            )
            .map_err(|error| {
                DbError::Config(format!(
                    "temporal join [{name}] key is not partitionable: {error}"
                ))
            })?,
        );
        let left_time_index = column_index(
            &left_schema,
            &config.left_time_column,
            name,
            "left event time",
        )?;
        let right_time_index = column_index(
            &right_schema,
            &config.right_time_column,
            name,
            "right event time",
        )?;
        let minimum_probe_offset = config
            .probe_schedule
            .offsets_ms()
            .iter()
            .copied()
            .min()
            .ok_or_else(|| DbError::Config("temporal probe schedule must not be empty".into()))?;
        let vnode_count = u32::from(key_group_count);
        let vnode_count_nonzero = NonZeroU32::new(vnode_count)
            .ok_or_else(|| DbError::Config("temporal vnode count must be nonzero".into()))?;
        let local_assignment =
            VnodeRegistry::single_owner(vnode_count, LOCAL_NODE_ID).versioned_snapshot();
        let projection_input_schema = temporal_join_output_schema(
            &left_schema,
            &right_schema,
            &config.right_table,
            config.join_kind,
            config.probe_alias.is_some(),
        )?;
        let operator = Self {
            name: Arc::from(name),
            projection: ProjectingJoinState::new(name, ctx, projection_sql, TEMPORAL_TMP_TABLE),
            config,
            limits,
            left_schema,
            right_schema,
            projection_input_schema,
            left_key_indices,
            right_key_indices,
            key_codec,
            vnode_count: vnode_count_nonzero,
            left_time_index,
            right_time_index,
            minimum_probe_offset,
            key_group_count,
            local_assignment,
            vnode_states: std::iter::repeat_with(|| None)
                .take(vnode_count as usize)
                .collect(),
            resident_vnodes: Vec::with_capacity(vnode_count as usize),
            vnode_pending_holds: vec![None; vnode_count as usize],
            pending_hold_counts: BTreeMap::new(),
            dirty_vnodes: vec![false; vnode_count as usize],
            dirty_vnode_roster: Vec::with_capacity(vnode_count as usize),
            vnode_capture_mode: VnodeCaptureMode::Full,
            retained_state_bytes: 0,
            max_managed_state_bytes: crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            frontiers: [InputFrontier::default(); 2],
            restored_frontiers: None,
            whole_restore: WholeRestoreState::Pending,
            pending_frontiers: None,
            frontier_cursor: 0,
            frontier_remaining: 0,
            frontier_has_work: false,
            maintenance_cursor: 0,
            maintenance_pending: false,
            maintenance_remaining: 0,
            maintenance_rescan: false,
            published_output_frontier: None,
            #[cfg(feature = "cluster")]
            left_stage: format!("{name}::left"),
            #[cfg(feature = "cluster")]
            right_stage: format!("{name}::right"),
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
        };
        let validation = operator.state_config(0, operator.max_managed_state_bytes);
        let _ = TemporalJoinVnodeState::try_new(
            Arc::clone(&operator.left_schema),
            Arc::clone(&operator.right_schema),
            validation,
        )?;
        Ok(operator)
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn attach_cluster_shuffle(&mut self, config: ClusterShuffleConfig) {
        debug_assert!(self.resident_vnodes.is_empty());
        debug_assert_eq!(config.registry.vnode_count(), self.vnode_count.get());
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
                "temporal join [{}] has no cluster shuffle scope",
                self.name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let sender_digest = config.sender.active_assignment_digest();
        let receiver_digest = config.receiver.active_assignment_digest();
        if u32::try_from(assignment.owners().len()).ok() != Some(self.vnode_count.get())
            || assignment.version() != self.local_assignment.version()
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
                "temporal join [{}] cluster ownership is outside its attached assignment",
                self.name
            )));
        }
        Ok((config, assignment, Arc::clone(&self.cluster_peers)))
    }

    #[cfg(feature = "cluster")]
    fn owner_map_digest(&self, assignment: &VnodeAssignmentSnapshot) -> [u8; 32] {
        let owners = assignment
            .owners()
            .iter()
            .map(|owner| owner.0)
            .collect::<Vec<_>>();
        CheckpointAssignmentFence::owner_map_digest(self.vnode_count.get(), &owners)
    }

    #[cfg(feature = "cluster")]
    fn capture_cluster_checkpoint(
        &self,
        max_capture_bytes: usize,
    ) -> Result<Option<CapturedTemporalClusterCheckpoint>, DbError> {
        let Some(cluster) = self.cluster_shuffle.as_ref() else {
            return Ok(None);
        };
        let (_, assignment, peers) = self.active_cluster_scope()?;
        let expected_peers = Self::remote_owner_peers(&assignment, cluster.self_id);
        if self.pending_cluster_input.is_some()
            || peers.as_ref() != expected_peers.as_slice()
            || self.last_broadcasts != self.local_frontiers
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] cluster frontier topology is not at a checkpoint boundary",
                self.name
            )));
        }
        let effective = self.effective_cluster_frontiers(self.local_frontiers, None, None)?;
        for (port, frontier) in effective.into_iter().enumerate() {
            if self.peer_channels[port]
                .values()
                .all(|channel| channel.events.is_empty())
                && frontier != self.frontiers[port]
            {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] applied {} cluster frontier is inconsistent",
                    self.name,
                    if port == 0 { "left" } else { "right" }
                )));
            }
        }

        let mut event_count = 0usize;
        let mut queued_bytes = 0usize;
        let mut capacity_bytes = 0usize;
        let mut retained_bytes = 0usize;
        for side in [TemporalInputSide::Left, TemporalInputSide::Right] {
            let port = side.port();
            if self.peer_channels[port].len() != peers.len() {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] {} checkpoint channel roster is incomplete",
                    self.name,
                    side.name()
                )));
            }
            let channel_roster_bytes = self.peer_channels[port]
                .len()
                .checked_mul(std::mem::size_of::<CapturedTemporalCheckpointChannel>())
                .and_then(|bytes| {
                    bytes.checked_add(usize::from(bytes != 0) * OPERATOR_CAPTURE_ALLOCATION_CHARGE)
                })
                .ok_or_else(|| self.accounting_error())?;
            retained_bytes = retained_bytes
                .checked_add(channel_roster_bytes)
                .ok_or_else(|| self.accounting_error())?;
            for (&peer, channel) in &self.peer_channels[port] {
                if peers.binary_search(&peer).is_err() {
                    return Err(DbError::Checkpoint(format!(
                        "temporal join [{}] {} checkpoint contains unknown peer {peer}",
                        self.name,
                        side.name()
                    )));
                }
                capacity_bytes = capacity_bytes
                    .checked_add(
                        channel
                            .events
                            .capacity()
                            .checked_mul(REMOTE_EVENT_CHARGE)
                            .ok_or_else(|| self.accounting_error())?,
                    )
                    .ok_or_else(|| self.accounting_error())?;
                let event_roster_bytes = channel
                    .events
                    .len()
                    .checked_mul(std::mem::size_of::<CapturedTemporalCheckpointEvent>())
                    .and_then(|bytes| {
                        bytes.checked_add(
                            usize::from(bytes != 0) * OPERATOR_CAPTURE_ALLOCATION_CHARGE,
                        )
                    })
                    .ok_or_else(|| self.accounting_error())?;
                retained_bytes = retained_bytes
                    .checked_add(event_roster_bytes)
                    .ok_or_else(|| self.accounting_error())?;
                if !channel.applied.idle {
                    validate_frontier(
                        self.frontiers[port],
                        channel.applied,
                        side.name(),
                        &self.name,
                    )?;
                }
                let mut accepted = channel.applied;
                let mut previous_recovery = None;
                for event in &channel.events {
                    if event.assignment_version != assignment.version()
                        || event.recovery_gen > cluster.receiver.recovery_gen()
                        || previous_recovery.is_some_and(|previous| event.recovery_gen < previous)
                    {
                        return Err(DbError::Checkpoint(format!(
                            "temporal join [{}] {} peer {peer} queue crosses assignment {}",
                            self.name,
                            side.name(),
                            assignment.version()
                        )));
                    }
                    previous_recovery = Some(event.recovery_gen);
                    event_count = event_count
                        .checked_add(1)
                        .ok_or_else(|| self.accounting_error())?;
                    queued_bytes = queued_bytes
                        .checked_add(event.payload_bytes())
                        .ok_or_else(|| self.accounting_error())?;
                    match &event.payload {
                        TemporalRemoteEventPayload::Data(batch) => {
                            if accepted.idle
                                || batch.retained.peer() != Some(peer)
                                || batch.retained.assignment_version()
                                    != Some(event.assignment_version)
                                || batch.retained.recovery_gen() != Some(event.recovery_gen)
                            {
                                return Err(DbError::Checkpoint(format!(
                                    "temporal join [{}] {} peer {peer} queue has invalid data scope",
                                    self.name,
                                    side.name()
                                )));
                            }
                            retained_bytes = retained_bytes
                                .checked_add(
                                    batch
                                        .retained
                                        .heap_bytes()
                                        .ok_or_else(|| self.accounting_error())?,
                                )
                                .and_then(|bytes| bytes.checked_add(RETAINED_BATCH_ARC_CHARGE))
                                .ok_or_else(|| self.accounting_error())?;
                        }
                        TemporalRemoteEventPayload::Frontier(frontier) => {
                            validate_frontier(accepted, *frontier, side.name(), &self.name)?;
                            if accepted.idle
                                && !frontier.idle
                                && frontier.watermark
                                    != max_watermark(
                                        frontier.watermark,
                                        self.frontiers[port].watermark,
                                    )
                            {
                                return Err(DbError::Checkpoint(format!(
                                    "temporal join [{}] {} peer {peer} revival is below the applied frontier",
                                    self.name,
                                    side.name()
                                )));
                            }
                            accepted = *frontier;
                        }
                    }
                }
                if accepted != channel.accepted {
                    return Err(DbError::Checkpoint(format!(
                        "temporal join [{}] {} peer {peer} accepted frontier is not derivable from its queue",
                        self.name,
                        side.name()
                    )));
                }
            }
        }
        if event_count != self.queued_remote_events
            || queued_bytes != self.queued_shuffle_bytes
            || capacity_bytes != self.queued_event_capacity_bytes
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] channel accounting is inconsistent",
                self.name
            )));
        }
        if retained_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] channel capture requires {retained_bytes} bytes; capture headroom is {max_capture_bytes} bytes",
                self.name
            )));
        }
        let mut channels = [
            Vec::with_capacity(self.peer_channels[0].len()),
            Vec::with_capacity(self.peer_channels[1].len()),
        ];
        for side in [TemporalInputSide::Left, TemporalInputSide::Right] {
            let port = side.port();
            for (&peer, channel) in &self.peer_channels[port] {
                let events = channel
                    .events
                    .iter()
                    .map(|event| match &event.payload {
                        TemporalRemoteEventPayload::Data(batch) => {
                            CapturedTemporalCheckpointEvent::Data {
                                recovery_gen: event.recovery_gen,
                                retained: Arc::clone(&batch.retained),
                                mutation_stream: batch.mutation_stream,
                            }
                        }
                        TemporalRemoteEventPayload::Frontier(frontier) => {
                            CapturedTemporalCheckpointEvent::Frontier {
                                recovery_gen: event.recovery_gen,
                                frontier: *frontier,
                            }
                        }
                    })
                    .collect();
                channels[port].push(CapturedTemporalCheckpointChannel {
                    peer,
                    applied: channel.applied,
                    events,
                });
            }
        }
        let mut actual_retained_bytes = retained_bytes;
        for side in &channels {
            actual_retained_bytes = actual_retained_bytes
                .checked_add(
                    side.capacity()
                        .saturating_sub(side.len())
                        .checked_mul(std::mem::size_of::<CapturedTemporalCheckpointChannel>())
                        .ok_or_else(|| self.accounting_error())?,
                )
                .ok_or_else(|| self.accounting_error())?;
            for channel in side {
                actual_retained_bytes = actual_retained_bytes
                    .checked_add(
                        channel
                            .events
                            .capacity()
                            .saturating_sub(channel.events.len())
                            .checked_mul(std::mem::size_of::<CapturedTemporalCheckpointEvent>())
                            .ok_or_else(|| self.accounting_error())?,
                    )
                    .ok_or_else(|| self.accounting_error())?;
            }
        }
        if actual_retained_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] channel capture retains {actual_retained_bytes} bytes; capture headroom is {max_capture_bytes} bytes",
                self.name
            )));
        }
        Ok(Some(CapturedTemporalClusterCheckpoint {
            assignment_version: assignment.version(),
            owner_map_digest: self.owner_map_digest(&assignment),
            self_id: cluster.self_id.0,
            local_frontiers: self.local_frontiers,
            remote_peer_cursors: self.remote_peer_cursors,
            channels,
            retained_bytes: actual_retained_bytes,
        }))
    }

    fn capture_operator_checkpoint(
        &self,
        max_capture_bytes: usize,
    ) -> Result<Option<TemporalJoinOperatorCheckpointCapture>, DbError> {
        #[cfg(feature = "cluster")]
        if self.pending_cluster_input.is_some() {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] cannot checkpoint while a shuffle send is pending",
                self.name
            )));
        }
        if self.pending_frontiers.is_some() {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] cannot checkpoint during bounded frontier fanout",
                self.name
            )));
        }
        self.validate_published_output_frontier(self.frontiers, self.published_output_frontier)?;
        #[cfg(feature = "cluster")]
        let cluster_attached = self.cluster_shuffle.is_some();
        #[cfg(not(feature = "cluster"))]
        let cluster_attached = false;
        if self.frontiers == [InputFrontier::default(); 2]
            && self.maintenance_cursor == 0
            && !self.maintenance_pending
            && self.maintenance_remaining == 0
            && !self.maintenance_rescan
            && self.published_output_frontier.is_none()
            && !cluster_attached
        {
            return Ok(None);
        }
        let base_bytes = std::mem::size_of::<TemporalJoinOperatorCheckpointCapture>();
        #[cfg(feature = "cluster")]
        let cluster = {
            let cluster_headroom = max_capture_bytes.checked_sub(base_bytes).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "temporal join [{}] operator capture metadata exceeds its {max_capture_bytes}-byte headroom",
                    self.name
                ))
            })?;
            self.capture_cluster_checkpoint(cluster_headroom)?
        };
        let maintenance_cursor = u32::try_from(self.maintenance_cursor).map_err(|_| {
            DbError::Checkpoint(format!(
                "temporal join [{}] maintenance cursor exceeds u32",
                self.name
            ))
        })?;
        let maintenance_remaining = u32::try_from(self.maintenance_remaining).map_err(|_| {
            DbError::Checkpoint(format!(
                "temporal join [{}] maintenance sweep exceeds u32",
                self.name
            ))
        })?;
        #[cfg(feature = "cluster")]
        let retained_bytes = cluster.as_ref().map_or(Ok(base_bytes), |cluster| {
            base_bytes
                .checked_add(cluster.retained_bytes())
                .ok_or_else(|| self.accounting_error())
        })?;
        #[cfg(not(feature = "cluster"))]
        let retained_bytes = base_bytes;
        if retained_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] operator capture retains {retained_bytes} bytes; capture headroom is {max_capture_bytes} bytes",
                self.name
            )));
        }
        Ok(Some(TemporalJoinOperatorCheckpointCapture {
            checkpoint: TemporalJoinOperatorCheckpoint {
                version: OPERATOR_CHECKPOINT_VERSION,
                frontiers: self.frontiers.map(Into::into),
                maintenance_cursor,
                maintenance_pending: self.maintenance_pending,
                maintenance_remaining,
                maintenance_rescan: self.maintenance_rescan,
                published_output_frontier: self.published_output_frontier.map(Into::into),
                cluster: None,
            },
            #[cfg(feature = "cluster")]
            cluster,
            retained_bytes,
        }))
    }

    #[cfg(feature = "cluster")]
    fn decoded_cluster_frontiers(
        &self,
        local: [InputFrontier; 2],
        channels: &[BTreeMap<u64, TemporalPeerChannel>; 2],
        floor: [InputFrontier; 2],
    ) -> Result<[InputFrontier; 2], DbError> {
        let merge = |port: usize| -> Result<InputFrontier, DbError> {
            let peers = channels[port].values().map(|channel| {
                let mut frontier = channel.applied;
                if !channel.events.is_empty() {
                    frontier.idle = false;
                    frontier.watermark = max_watermark(frontier.watermark, floor[port].watermark);
                }
                frontier
            });
            let merged =
                merge_input_frontier_iter(std::iter::once(local[port]).chain(peers), i64::MIN);
            validate_frontier(
                floor[port],
                merged,
                if port == 0 { "left" } else { "right" },
                &self.name,
            )?;
            Ok(merged)
        };
        Ok([merge(0)?, merge(1)?])
    }

    #[cfg(feature = "cluster")]
    fn ensure_cluster_restore_budget(
        &self,
        base: usize,
        payload: usize,
        event_capacity: usize,
    ) -> Result<(), DbError> {
        let proposed = base
            .checked_add(payload)
            .and_then(|bytes| bytes.checked_add(event_capacity))
            .ok_or_else(|| self.accounting_error())?;
        if proposed > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] cluster checkpoint restore", self.name),
                accounted_bytes: proposed,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn decoded_cluster_runtime_bytes(
        &self,
        cluster: &DecodedTemporalCluster,
    ) -> Result<usize, DbError> {
        cluster
            .peer_channels
            .iter()
            .map(BTreeMap::len)
            .sum::<usize>()
            .checked_mul(
                std::mem::size_of::<(u64, TemporalPeerChannel)>()
                    .checked_add(PENDING_HOLD_ENTRY_CHARGE)
                    .ok_or_else(|| self.accounting_error())?,
            )
            .and_then(|bytes| bytes.checked_add(cluster.queued_event_capacity_bytes))
            .and_then(|bytes| bytes.checked_add(cluster.queued_shuffle_bytes))
            .ok_or_else(|| self.accounting_error())
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_stream_reader<'a>(
        &self,
        side: TemporalInputSide,
        peer: u64,
        stream: &str,
        has_events: bool,
        ipc: &'a [u8],
    ) -> Result<Option<StreamReader<std::io::Cursor<&'a [u8]>>>, DbError> {
        if !has_events {
            if !ipc.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] {} peer {peer} has {stream} IPC without data events",
                    self.name,
                    side.name()
                )));
            }
            return Ok(None);
        }
        StreamReader::try_new(std::io::Cursor::new(ipc), None)
            .map(Some)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "temporal join [{}] {} peer {peer} {stream} channel IPC restore: {error}",
                    self.name,
                    side.name()
                ))
            })
    }

    #[cfg(feature = "cluster")]
    fn decode_cluster_checkpoint(
        &self,
        checkpoint: TemporalClusterCheckpoint,
        saved_frontiers: [InputFrontier; 2],
        restore_scratch_bytes: usize,
    ) -> Result<DecodedTemporalCluster, DbError> {
        let (config, assignment, peers) = self.active_cluster_scope()?;
        let expected_peers = Self::remote_owner_peers(&assignment, config.self_id);
        if peers.as_ref() != expected_peers.as_slice()
            || checkpoint.assignment_version != assignment.version()
            || checkpoint.owner_map_digest != self.owner_map_digest(&assignment)
            || checkpoint.self_id != config.self_id.0
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] cluster checkpoint does not match assignment {}",
                self.name,
                assignment.version()
            )));
        }

        let local_frontiers: [InputFrontier; 2] = checkpoint.local_frontiers.map(Into::into);
        let checkpoint_assignment = checkpoint.assignment_version;
        for (port, frontier) in local_frontiers.iter().enumerate() {
            if frontier.watermark == Some(i64::MIN) {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] cluster checkpoint frontier {port} uses the uninitialized sentinel",
                    self.name
                )));
            }
        }
        if checkpoint
            .remote_peer_cursors
            .iter()
            .flatten()
            .any(|peer| peers.binary_search(peer).is_err())
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] cluster checkpoint has an invalid broadcast boundary",
                self.name
            )));
        }

        let mut peer_channels = [BTreeMap::new(), BTreeMap::new()];
        let mut queued_shuffle_bytes = 0usize;
        let mut queued_remote_events = 0usize;
        let mut queued_event_capacity_bytes = 0usize;
        let restore_base = self
            .checked_accounted_state_bytes()?
            .checked_add(restore_scratch_bytes)
            .ok_or_else(|| self.accounting_error())?;
        self.ensure_cluster_restore_budget(restore_base, 0, 0)?;
        for (port, archived_channels) in checkpoint.channels.into_iter().enumerate() {
            let side = if port == 0 {
                TemporalInputSide::Left
            } else {
                TemporalInputSide::Right
            };
            if archived_channels.len() != peers.len() {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] {} checkpoint peer roster is incomplete",
                    self.name,
                    side.name()
                )));
            }
            for (expected_peer, archived) in peers.iter().zip(archived_channels) {
                let TemporalCheckpointChannel {
                    peer,
                    applied,
                    events: archived_events,
                    positioned_ipc,
                    mutation_ipc,
                } = archived;
                if peer != *expected_peer {
                    return Err(DbError::Checkpoint(format!(
                        "temporal join [{}] {} checkpoint peers are not canonical",
                        self.name,
                        side.name()
                    )));
                }
                let applied: InputFrontier = applied.into();
                if applied.watermark == Some(i64::MIN) {
                    return Err(DbError::Checkpoint(format!(
                        "temporal join [{}] {} peer {} has an invalid applied frontier",
                        self.name,
                        side.name(),
                        peer
                    )));
                }
                if !applied.idle {
                    validate_frontier(saved_frontiers[port], applied, side.name(), &self.name)
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "temporal join [{}] {} peer {peer} applied frontier: {error}",
                                self.name,
                                side.name()
                            ))
                        })?;
                }
                let minimum_capacity = archived_events
                    .len()
                    .checked_mul(REMOTE_EVENT_CHARGE)
                    .and_then(|bytes| queued_event_capacity_bytes.checked_add(bytes))
                    .ok_or_else(|| self.accounting_error())?;
                self.ensure_cluster_restore_budget(
                    restore_base,
                    queued_shuffle_bytes,
                    minimum_capacity,
                )?;
                let mut events = VecDeque::new();
                events
                    .try_reserve_exact(archived_events.len())
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "temporal join [{}] {} peer {} queue reservation: {error}",
                            self.name,
                            side.name(),
                            peer
                        ))
                    })?;
                queued_event_capacity_bytes = queued_event_capacity_bytes
                    .checked_add(
                        events
                            .capacity()
                            .checked_mul(REMOTE_EVENT_CHARGE)
                            .ok_or_else(|| self.accounting_error())?,
                    )
                    .ok_or_else(|| self.accounting_error())?;
                self.ensure_cluster_restore_budget(
                    restore_base,
                    queued_shuffle_bytes,
                    queued_event_capacity_bytes,
                )?;
                let mut has_stream = [false; 2];
                for event in &archived_events {
                    if let TemporalCheckpointEvent::Data {
                        mutation_stream, ..
                    } = event
                    {
                        if matches!(side, TemporalInputSide::Left) && *mutation_stream {
                            return Err(DbError::Checkpoint(format!(
                                "temporal join [{}] left peer {peer} uses a mutation stream",
                                self.name
                            )));
                        }
                        has_stream[usize::from(*mutation_stream)] = true;
                    }
                }
                let mut positioned_reader = self.checkpoint_stream_reader(
                    side,
                    peer,
                    "positioned",
                    has_stream[0],
                    &positioned_ipc,
                )?;
                let mut mutation_reader = self.checkpoint_stream_reader(
                    side,
                    peer,
                    "mutation",
                    has_stream[1],
                    &mutation_ipc,
                )?;
                let mut accepted = applied;
                let mut previous_recovery = None;
                for archived_event in archived_events {
                    let event = match archived_event {
                        TemporalCheckpointEvent::Data {
                            recovery_gen,
                            routed_vnodes,
                            row_count,
                            mutation_stream,
                        } => {
                            if row_count == 0
                                || row_count
                                    > u64::try_from(laminar_core::shuffle::ROUTE_MAX_BATCH_ROWS)
                                        .unwrap_or(u64::MAX)
                                || routed_vnodes.is_empty()
                                || routed_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
                                || routed_vnodes.iter().any(|vnode| {
                                    assignment.owners().get(*vnode as usize)
                                        != Some(&config.self_id)
                                })
                                || recovery_gen > config.receiver.recovery_gen()
                                || previous_recovery.is_some_and(|previous| recovery_gen < previous)
                            {
                                return Err(DbError::Checkpoint(format!(
                                    "temporal join [{}] {} peer {} data is outside its assignment limits",
                                    self.name,
                                    side.name(),
                                    peer
                                )));
                            }
                            previous_recovery = Some(recovery_gen);
                            let stream = if mutation_stream {
                                "mutation"
                            } else {
                                "positioned"
                            };
                            let reader = if mutation_stream {
                                &mut mutation_reader
                            } else {
                                &mut positioned_reader
                            };
                            let batch = match reader.as_mut().and_then(Iterator::next) {
                                Some(Ok(batch)) => batch,
                                Some(Err(error)) => {
                                    return Err(DbError::Checkpoint(format!(
                                        "temporal join [{}] {} peer {peer} {stream} channel IPC restore: {error}",
                                        self.name,
                                        side.name()
                                    )));
                                }
                                None => {
                                    return Err(DbError::Checkpoint(format!(
                                        "temporal join [{}] {} peer {peer} {stream} channel IPC has fewer batches than data events",
                                        self.name,
                                        side.name()
                                    )));
                                }
                            };
                            let logical_bytes = laminar_core::shuffle::logical_batch_bytes(&batch)
                                .map_err(|error| {
                                    DbError::Checkpoint(format!(
                                        "temporal join [{}] {} peer {} restored batch size: {error}",
                                        self.name,
                                        side.name(),
                                        peer
                                    ))
                                })?;
                            if u64::try_from(batch.num_rows()).ok() != Some(row_count)
                                || logical_bytes > laminar_core::shuffle::ROUTE_MAX_BATCH_BYTES
                            {
                                return Err(DbError::Checkpoint(format!(
                                    "temporal join [{}] {} peer {} restored batch exceeds its declared route limits",
                                    self.name,
                                    side.name(),
                                    peer
                                )));
                            }
                            let retained = crate::operator::RetainedBatch::restored_channel(
                                batch,
                                peer,
                                checkpoint_assignment,
                                recovery_gen,
                                routed_vnodes.into(),
                            );
                            let batch = self
                                .build_queued_batch(
                                    side,
                                    retained,
                                    accepted,
                                    &assignment,
                                    config.self_id,
                                )
                                .map_err(|error| {
                                    DbError::Checkpoint(format!(
                                        "temporal join [{}] {} peer {} restored data: {error}",
                                        self.name,
                                        side.name(),
                                        peer
                                    ))
                                })?;
                            if batch.mutation_stream != mutation_stream {
                                return Err(DbError::Checkpoint(format!(
                                    "temporal join [{}] {} peer {peer} data stream does not match its decoded schema",
                                    self.name,
                                    side.name()
                                )));
                            }
                            TemporalRemoteEvent {
                                assignment_version: checkpoint_assignment,
                                recovery_gen,
                                payload: TemporalRemoteEventPayload::Data(batch),
                            }
                        }
                        TemporalCheckpointEvent::Frontier {
                            recovery_gen,
                            frontier,
                        } => {
                            let frontier: InputFrontier = frontier.into();
                            if frontier.watermark == Some(i64::MIN)
                                || recovery_gen > config.receiver.recovery_gen()
                                || previous_recovery.is_some_and(|previous| recovery_gen < previous)
                            {
                                return Err(DbError::Checkpoint(format!(
                                    "temporal join [{}] {} peer {} frontier is outside its assignment",
                                    self.name,
                                    side.name(),
                                    peer
                                )));
                            }
                            previous_recovery = Some(recovery_gen);
                            validate_frontier(accepted, frontier, side.name(), &self.name)
                                .map_err(|error| {
                                    DbError::Checkpoint(format!(
                                        "temporal join [{}] {} peer {} restored frontier: {error}",
                                        self.name,
                                        side.name(),
                                        peer
                                    ))
                                })?;
                            if accepted.idle && !frontier.idle {
                                let normalized = InputFrontier {
                                    watermark: max_watermark(
                                        frontier.watermark,
                                        saved_frontiers[port].watermark,
                                    ),
                                    idle: false,
                                };
                                if frontier != normalized {
                                    return Err(DbError::Checkpoint(format!(
                                        "temporal join [{}] {} peer {} revival is below the saved frontier",
                                        self.name,
                                        side.name(),
                                        peer
                                    )));
                                }
                            }
                            accepted = frontier;
                            TemporalRemoteEvent {
                                assignment_version: checkpoint_assignment,
                                recovery_gen,
                                payload: TemporalRemoteEventPayload::Frontier(frontier),
                            }
                        }
                    };
                    queued_shuffle_bytes = queued_shuffle_bytes
                        .checked_add(event.payload_bytes())
                        .ok_or_else(|| self.accounting_error())?;
                    queued_remote_events = queued_remote_events
                        .checked_add(1)
                        .ok_or_else(|| self.accounting_error())?;
                    self.ensure_cluster_restore_budget(
                        restore_base,
                        queued_shuffle_bytes,
                        queued_event_capacity_bytes,
                    )?;
                    events.push_back(event);
                }
                for (stream, reader) in [
                    ("positioned", &mut positioned_reader),
                    ("mutation", &mut mutation_reader),
                ] {
                    if let Some(reader) = reader.as_mut() {
                        match reader.next() {
                            None => {}
                            Some(Ok(_)) => {
                                return Err(DbError::Checkpoint(format!(
                                    "temporal join [{}] {} peer {peer} {stream} channel IPC has more batches than data events",
                                    self.name,
                                    side.name()
                                )));
                            }
                            Some(Err(error)) => {
                                return Err(DbError::Checkpoint(format!(
                                    "temporal join [{}] {} peer {peer} trailing {stream} channel IPC: {error}",
                                    self.name,
                                    side.name()
                                )));
                            }
                        }
                    }
                }
                peer_channels[port].insert(
                    peer,
                    TemporalPeerChannel {
                        applied,
                        accepted,
                        events,
                    },
                );
            }
        }
        let effective =
            self.decoded_cluster_frontiers(local_frontiers, &peer_channels, saved_frontiers)?;
        for (port, frontier) in effective.into_iter().enumerate() {
            if peer_channels[port]
                .values()
                .all(|channel| channel.events.is_empty())
                && frontier != saved_frontiers[port]
            {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] restored {} cluster frontier is inconsistent",
                    self.name,
                    if port == 0 { "left" } else { "right" }
                )));
            }
        }
        Ok(DecodedTemporalCluster {
            local_frontiers,
            peer_channels,
            remote_peer_cursors: checkpoint.remote_peer_cursors,
            queued_shuffle_bytes,
            queued_remote_events,
            queued_event_capacity_bytes,
        })
    }

    #[cfg(feature = "cluster")]
    fn stage_for_side(&self, side: TemporalInputSide) -> &str {
        match side {
            TemporalInputSide::Left => &self.left_stage,
            TemporalInputSide::Right => &self.right_stage,
        }
    }

    #[cfg(feature = "cluster")]
    fn side_for_stage(&self, stage: &str) -> Result<TemporalInputSide, DbError> {
        if stage == self.left_stage {
            Ok(TemporalInputSide::Left)
        } else if stage == self.right_stage {
            Ok(TemporalInputSide::Right)
        } else {
            Err(DbError::ShuffleTerminal(format!(
                "temporal join [{}] rejected unknown shuffle stage '{stage}'",
                self.name
            )))
        }
    }

    fn state_config(&self, vnode: u32, max_retained_bytes: usize) -> TemporalJoinStateConfig {
        TemporalJoinStateConfig {
            vnode,
            vnode_count: self.vnode_count,
            left_key_indices: self.left_key_indices.clone(),
            right_key_indices: self.right_key_indices.clone(),
            key_codec: Arc::clone(&self.key_codec),
            left_time_index: self.left_time_index,
            right_time_index: self.right_time_index,
            left_name: self.config.left_table.clone(),
            right_name: self.config.right_table.clone(),
            operator_name: self.name.to_string(),
            join_kind: self.config.join_kind,
            schedule: self.config.probe_schedule.clone(),
            emit_probe_metadata: self.config.probe_alias.is_some(),
            left_allowed_lateness_ms: self.limits.left_allowed_lateness_ms,
            right_allowed_lateness_ms: self.limits.right_allowed_lateness_ms,
            history_retention_ms: self.limits.history_retention_ms,
            limits: TemporalStateLimits {
                max_retained_bytes,
                max_pending_probes: self.limits.max_pending_probes,
                max_offsets_per_row: MAX_TEMPORAL_PROBES_PER_ROW,
                max_horizon_ms: MAX_TEMPORAL_PROBE_HORIZON_MS,
            },
        }
    }

    fn vnode_state_frontiers(state: &TemporalJoinVnodeState) -> [InputFrontier; 2] {
        let (left_watermark, left_idle, right_watermark, right_idle) = state.frontier_snapshot();
        [
            InputFrontier {
                watermark: left_watermark,
                idle: left_idle,
            },
            InputFrontier {
                watermark: right_watermark,
                idle: right_idle,
            },
        ]
    }

    fn preflight_whole_checkpoint_archive(
        &self,
        bytes: &[u8],
        context: &str,
        validate: impl FnOnce(&ArchivedTemporalJoinOperatorCheckpoint) -> Result<(), DbError>,
    ) -> Result<TemporalWholeRestorePreflight, DbError> {
        with_aligned_checkpoint_bytes(bytes, |bytes| {
            let archived =
                rkyv::access::<ArchivedTemporalJoinOperatorCheckpoint, rkyv::rancor::Error>(bytes)
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "temporal join [{}] {context} archive validation: {error}",
                            self.name
                        ))
                    })?;
            if archived.version != OPERATOR_CHECKPOINT_VERSION {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] {context} uses unsupported checkpoint version {}",
                    self.name, archived.version
                )));
            }
            validate(archived)?;

            let mut decoded_bytes = std::mem::size_of::<TemporalJoinOperatorCheckpoint>();
            let mut add = |charge: usize| -> Result<(), DbError> {
                decoded_bytes = decoded_bytes.checked_add(charge).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "temporal join [{}] {context} decoded-checkpoint accounting overflow",
                        self.name
                    ))
                })?;
                Ok(())
            };
            #[cfg(feature = "cluster")]
            let mut runtime_scratch = 0usize;
            #[cfg(not(feature = "cluster"))]
            let runtime_scratch = 0usize;
            #[cfg(feature = "cluster")]
            let mut add_runtime = |charge: usize| -> Result<(), DbError> {
                runtime_scratch = runtime_scratch.checked_add(charge).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "temporal join [{}] {context} runtime-scratch accounting overflow",
                        self.name
                    ))
                })?;
                Ok(())
            };
            if let Some(cluster) = archived.cluster.as_ref() {
                for (port, channels) in cluster.channels.iter().enumerate() {
                    add(checkpoint_roster_bytes(
                        channels.len(),
                        std::mem::size_of::<TemporalCheckpointChannel>(),
                    )?)?;
                    #[cfg(feature = "cluster")]
                    add_runtime(
                        channels
                            .len()
                            .checked_mul(
                                std::mem::size_of::<(u64, TemporalPeerChannel)>()
                                    .checked_add(PENDING_HOLD_ENTRY_CHARGE)
                                    .ok_or_else(|| self.accounting_error())?,
                            )
                            .ok_or_else(|| self.accounting_error())?,
                    )?;
                    for channel in channels.iter() {
                        add(checkpoint_roster_bytes(
                            channel.events.len(),
                            std::mem::size_of::<TemporalCheckpointEvent>(),
                        )?)?;
                        add(checkpoint_allocation_bytes(channel.positioned_ipc.len())?)?;
                        add(checkpoint_allocation_bytes(channel.mutation_ipc.len())?)?;
                        #[cfg(feature = "cluster")]
                        add_runtime(checkpoint_allocation_bytes(
                            channel
                                .events
                                .len()
                                .checked_mul(REMOTE_EVENT_CHARGE)
                                .ok_or_else(|| self.accounting_error())?,
                        )?)?;
                        #[cfg(feature = "cluster")]
                        let positioned_preflight = preflight_temporal_channel_ipc_restore(
                            channel.positioned_ipc.as_slice(),
                            channel.events.iter().filter_map(|event| match event {
                                ArchivedTemporalCheckpointEvent::Data {
                                    row_count,
                                    mutation_stream: false,
                                    ..
                                } => Some(row_count.to_native()),
                                ArchivedTemporalCheckpointEvent::Data { .. }
                                | ArchivedTemporalCheckpointEvent::Frontier { .. } => None,
                            }),
                            &format!(
                                "temporal join [{}] {} peer {} positioned channel",
                                self.name,
                                if port == 0 { "left" } else { "right" },
                                channel.peer
                            ),
                        )?;
                        #[cfg(feature = "cluster")]
                        let mutation_preflight = preflight_temporal_channel_ipc_restore(
                            channel.mutation_ipc.as_slice(),
                            channel.events.iter().filter_map(|event| match event {
                                ArchivedTemporalCheckpointEvent::Data {
                                    row_count,
                                    mutation_stream: true,
                                    ..
                                } => Some(row_count.to_native()),
                                ArchivedTemporalCheckpointEvent::Data { .. }
                                | ArchivedTemporalCheckpointEvent::Frontier { .. } => None,
                            }),
                            &format!(
                                "temporal join [{}] {} peer {} mutation channel",
                                self.name,
                                if port == 0 { "left" } else { "right" },
                                channel.peer
                            ),
                        )?;
                        #[cfg(feature = "cluster")]
                        {
                            if port == 0 && mutation_preflight.rows != 0 {
                                return Err(DbError::Checkpoint(format!(
                                    "temporal join [{}] left peer {} uses a mutation stream",
                                    self.name, channel.peer
                                )));
                            }
                            let ipc_bytes = channel
                                .positioned_ipc
                                .len()
                                .checked_add(channel.mutation_ipc.len())
                                .and_then(|bytes| {
                                    bytes.checked_mul(WHOLE_RESTORE_IPC_EXPANSION_FACTOR)
                                })
                                .ok_or_else(|| self.accounting_error())?;
                            let decoded_rows = positioned_preflight
                                .scratch_rows
                                .checked_add(mutation_preflight.scratch_rows)
                                .ok_or_else(|| self.accounting_error())?;
                            let row_scratch = decoded_rows
                                .checked_mul(WHOLE_RESTORE_ROW_SCRATCH_CHARGE)
                                .ok_or_else(|| self.accounting_error())?;
                            let body_bytes = positioned_preflight
                                .body_bytes
                                .checked_add(mutation_preflight.body_bytes)
                                .ok_or_else(|| self.accounting_error())?;
                            if body_bytes
                                > channel
                                    .positioned_ipc
                                    .len()
                                    .saturating_add(channel.mutation_ipc.len())
                            {
                                return Err(DbError::Checkpoint(format!(
                                    "temporal join [{}] peer {} IPC body accounting is invalid",
                                    self.name, channel.peer
                                )));
                            }
                            add_runtime(
                                ipc_bytes
                                    .checked_add(row_scratch)
                                    .ok_or_else(|| self.accounting_error())?,
                            )?;
                        }
                        for event in channel.events.iter() {
                            if let ArchivedTemporalCheckpointEvent::Data {
                                routed_vnodes,
                                row_count,
                                mutation_stream,
                                ..
                            } = event
                            {
                                let row_count =
                                    usize::try_from(row_count.to_native()).map_err(|_| {
                                        DbError::Checkpoint(format!(
                                            "temporal join [{}] {context} row count exceeds usize",
                                            self.name
                                        ))
                                    })?;
                                if row_count == 0
                                    || row_count > laminar_core::shuffle::ROUTE_MAX_BATCH_ROWS
                                    || routed_vnodes.is_empty()
                                    || routed_vnodes
                                        .as_slice()
                                        .windows(2)
                                        .any(|pair| pair[0] >= pair[1])
                                    || routed_vnodes
                                        .iter()
                                        .any(|vnode| *vnode >= u32::from(self.key_group_count))
                                    || (port == 0 && *mutation_stream)
                                {
                                    return Err(DbError::Checkpoint(format!(
                                        "temporal join [{}] {context} queued data shape is invalid",
                                        self.name
                                    )));
                                }
                                add(checkpoint_roster_bytes(
                                    routed_vnodes.len(),
                                    std::mem::size_of::<u32>(),
                                )?)?;
                                #[cfg(feature = "cluster")]
                                {
                                    let key_columns = if port == 0 {
                                        self.left_key_indices.len()
                                    } else {
                                        self.right_key_indices.len()
                                    };
                                    let route_scratch = routed_vnodes
                                        .len()
                                        .checked_add(
                                            row_count
                                                .checked_mul(2)
                                                .ok_or_else(|| self.accounting_error())?,
                                        )
                                        .and_then(|count| {
                                            count.checked_mul(std::mem::size_of::<u32>())
                                        })
                                        .and_then(|bytes| {
                                            key_columns
                                                .checked_mul(std::mem::size_of::<
                                                    Arc<dyn arrow::array::Array>,
                                                >(
                                                ))
                                                .and_then(|columns| bytes.checked_add(columns))
                                        })
                                        .and_then(|bytes| {
                                            bytes.checked_add(RETAINED_BATCH_ARC_CHARGE)
                                        })
                                        .and_then(|bytes| {
                                            bytes
                                                .checked_add(6 * OPERATOR_CAPTURE_ALLOCATION_CHARGE)
                                        })
                                        .ok_or_else(|| self.accounting_error())?;
                                    add_runtime(route_scratch)?;
                                }
                            }
                        }
                    }
                }
            }
            Ok(TemporalWholeRestorePreflight {
                decoded_checkpoint: decoded_bytes,
                runtime_scratch,
            })
        })
    }

    fn decoded_whole_checkpoint_bytes(
        checkpoint: &TemporalJoinOperatorCheckpoint,
    ) -> Result<usize, DbError> {
        let mut decoded_bytes = std::mem::size_of::<TemporalJoinOperatorCheckpoint>();
        let mut add = |charge: usize| -> Result<(), DbError> {
            decoded_bytes = decoded_bytes.checked_add(charge).ok_or_else(|| {
                DbError::Checkpoint("temporal decoded whole-checkpoint accounting overflow".into())
            })?;
            Ok(())
        };
        if let Some(cluster) = &checkpoint.cluster {
            for channels in &cluster.channels {
                add(checkpoint_roster_bytes(
                    channels.capacity(),
                    std::mem::size_of::<TemporalCheckpointChannel>(),
                )?)?;
                for channel in channels {
                    add(checkpoint_roster_bytes(
                        channel.events.capacity(),
                        std::mem::size_of::<TemporalCheckpointEvent>(),
                    )?)?;
                    add(checkpoint_allocation_bytes(
                        channel.positioned_ipc.capacity(),
                    )?)?;
                    add(checkpoint_allocation_bytes(
                        channel.mutation_ipc.capacity(),
                    )?)?;
                    for event in &channel.events {
                        if let TemporalCheckpointEvent::Data { routed_vnodes, .. } = event {
                            add(checkpoint_roster_bytes(
                                routed_vnodes.capacity(),
                                std::mem::size_of::<u32>(),
                            )?)?;
                        }
                    }
                }
            }
        }
        Ok(decoded_bytes)
    }

    fn decode_vnode_frame(
        &self,
        vnode: u32,
        vnode_count: u32,
        bytes: &[u8],
        max_state_bytes: usize,
    ) -> Result<Option<Box<TemporalJoinVnodeState>>, DbError> {
        if vnode_count != u32::from(self.key_group_count) || vnode >= vnode_count {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] vnode {vnode} restore does not match its {vnode_count}-vnode topology",
                self.name
            )));
        }
        let (&tag, payload) = bytes.split_first().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "temporal join [{}] vnode {vnode} frame is empty",
                self.name
            ))
        })?;
        match tag {
            ABSENT_VNODE if payload.is_empty() => Ok(None),
            ABSENT_VNODE => Err(DbError::Checkpoint(format!(
                "temporal join [{}] absent vnode {vnode} frame has a payload",
                self.name
            ))),
            PRESENT_VNODE if payload.is_empty() => Err(DbError::Checkpoint(format!(
                "temporal join [{}] present vnode {vnode} frame has no payload",
                self.name
            ))),
            PRESENT_VNODE => {
                let config = self.state_config(vnode, max_state_bytes);
                TemporalJoinVnodeState::restore(
                    Arc::clone(&self.left_schema),
                    Arc::clone(&self.right_schema),
                    config,
                    payload,
                )
                .map(Box::new)
                .map(Some)
            }
            _ => Err(DbError::Checkpoint(format!(
                "temporal join [{}] vnode {vnode} frame has unknown tag {tag}",
                self.name
            ))),
        }
    }

    fn topology_charge(&self) -> Result<usize, DbError> {
        let vnode_slots = self
            .vnode_states
            .capacity()
            .checked_mul(std::mem::size_of::<Option<Box<TemporalJoinVnodeState>>>())
            .ok_or_else(|| self.accounting_error())?;
        let resident_roster = self
            .resident_vnodes
            .capacity()
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| self.accounting_error())?;
        let pending_holds = self
            .vnode_pending_holds
            .capacity()
            .checked_mul(std::mem::size_of::<Option<i64>>())
            .and_then(|bytes| {
                bytes.checked_add(
                    self.pending_hold_counts
                        .len()
                        .checked_mul(PENDING_HOLD_ENTRY_CHARGE)?,
                )
            })
            .ok_or_else(|| self.accounting_error())?;
        let assignment = self
            .local_assignment
            .owners()
            .len()
            .checked_mul(std::mem::size_of::<NodeId>() + std::mem::size_of::<u64>())
            .ok_or_else(|| self.accounting_error())?;
        let key_indices = self
            .left_key_indices
            .capacity()
            .checked_add(self.right_key_indices.capacity())
            .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<usize>()))
            .ok_or_else(|| self.accounting_error())?;
        let configured_keys = self
            .config
            .left_key_columns
            .capacity()
            .checked_add(self.config.right_key_columns.capacity())
            .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<String>()))
            .and_then(|bytes| {
                self.config
                    .left_key_columns
                    .iter()
                    .chain(&self.config.right_key_columns)
                    .try_fold(bytes, |total, column| total.checked_add(column.capacity()))
            })
            .ok_or_else(|| self.accounting_error())?;
        let capture_tracking = self
            .dirty_vnodes
            .capacity()
            .div_ceil(8)
            .checked_add(
                self.dirty_vnode_roster
                    .capacity()
                    .checked_mul(std::mem::size_of::<u32>())
                    .ok_or_else(|| self.accounting_error())?,
            )
            .ok_or_else(|| self.accounting_error())?;
        let strings = self
            .name
            .len()
            .checked_add(self.config.left_table.capacity())
            .and_then(|bytes| bytes.checked_add(self.config.right_table.capacity()))
            .and_then(|bytes| bytes.checked_add(self.config.left_time_column.capacity()))
            .and_then(|bytes| bytes.checked_add(self.config.right_time_column.capacity()))
            .and_then(|bytes| {
                bytes.checked_add(self.config.probe_alias.as_ref().map_or(0, String::capacity))
            })
            .ok_or_else(|| self.accounting_error())?;
        let schedule = self
            .config
            .probe_schedule
            .offsets_ms()
            .len()
            .checked_mul(std::mem::size_of::<i64>())
            .ok_or_else(|| self.accounting_error())?;
        let total = [
            vnode_slots,
            resident_roster,
            pending_holds,
            assignment,
            key_indices,
            configured_keys,
            capture_tracking,
            strings,
            schedule,
        ]
        .into_iter()
        .try_fold(std::mem::size_of::<Self>(), |total, bytes| {
            total
                .checked_add(bytes)
                .ok_or_else(|| self.accounting_error())
        })?;
        #[cfg(feature = "cluster")]
        let total = total
            .checked_add(self.cluster_topology_charge()?)
            .ok_or_else(|| self.accounting_error())?;
        Ok(total)
    }

    #[cfg(feature = "cluster")]
    fn cluster_topology_charge(&self) -> Result<usize, DbError> {
        let stages = self
            .left_stage
            .capacity()
            .checked_add(self.right_stage.capacity())
            .ok_or_else(|| self.accounting_error())?;
        let peers = self
            .cluster_peers
            .len()
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| self.accounting_error())?;
        let channels = self
            .cluster_peers
            .len()
            .checked_mul(2)
            .and_then(|entries| {
                entries.checked_mul(
                    std::mem::size_of::<(u64, TemporalPeerChannel)>()
                        .checked_add(PENDING_HOLD_ENTRY_CHARGE)?,
                )
            })
            .and_then(|bytes| bytes.checked_add(self.queued_event_capacity_bytes))
            .ok_or_else(|| self.accounting_error())?;
        stages
            .checked_add(peers)
            .and_then(|total| total.checked_add(channels))
            .ok_or_else(|| self.accounting_error())
    }

    fn checked_accounted_state_bytes(&self) -> Result<usize, DbError> {
        let accounted = self
            .topology_charge()?
            .checked_add(self.retained_state_bytes)
            .ok_or_else(|| self.accounting_error())?;
        #[cfg(feature = "cluster")]
        let accounted = accounted
            .checked_add(self.queued_shuffle_bytes)
            .and_then(|bytes| {
                bytes.checked_add(
                    self.pending_cluster_input
                        .as_ref()
                        .map_or(0, |pending| pending.accounted_bytes),
                )
            })
            .ok_or_else(|| self.accounting_error())?;
        Ok(accounted)
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
    fn cluster_input_plan_bytes(&self, plan: &ClusterInputPlan) -> Result<usize, DbError> {
        let routed_entries = plan
            .routed
            .len()
            .checked_mul(
                std::mem::size_of::<(u32, [Vec<RoutedTemporalBatch>; 2])>()
                    + PENDING_HOLD_ENTRY_CHARGE,
            )
            .ok_or_else(|| self.accounting_error())?;
        let mut bytes = plan
            .outbound
            .capacity()
            .checked_mul(std::mem::size_of::<(
                u64,
                laminar_core::shuffle::ShuffleMessage,
            )>())
            .and_then(|outbound| outbound.checked_add(routed_entries))
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
        let mut charged_keys = rustc_hash::FxHashSet::default();
        for sides in plan.routed.values() {
            for side in sides {
                bytes = bytes
                    .checked_add(
                        side.capacity()
                            .checked_mul(std::mem::size_of::<RoutedTemporalBatch>())
                            .ok_or_else(|| self.accounting_error())?,
                    )
                    .ok_or_else(|| self.accounting_error())?;
                for routed in side {
                    bytes = bytes
                        .checked_add(self.batch_plan_bytes(&routed.batch)?)
                        .ok_or_else(|| self.accounting_error())?;
                    if charged_keys.insert(Arc::as_ptr(&routed.keys)) {
                        bytes = bytes
                            .checked_add(routed.keys.size())
                            .and_then(|value| value.checked_add(2 * std::mem::size_of::<usize>()))
                            .ok_or_else(|| self.accounting_error())?;
                    }
                    let source_rows = routed
                        .source_rows
                        .len()
                        .checked_mul(std::mem::size_of::<u32>())
                        .and_then(|value| value.checked_add(2 * std::mem::size_of::<usize>()))
                        .ok_or_else(|| self.accounting_error())?;
                    bytes = bytes
                        .checked_add(source_rows)
                        .ok_or_else(|| self.accounting_error())?;
                }
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
                    .and_then(|value| {
                        routed_vnodes
                            .len()
                            .checked_mul(std::mem::size_of::<u32>())
                            .and_then(|routes| value.checked_add(routes))
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
    fn transition_accounted_bytes(transition: &PreparedTemporalJoinTransition) -> usize {
        let slots = transition
            .slots
            .capacity()
            .saturating_mul(std::mem::size_of::<(
                u32,
                Option<Box<TemporalJoinVnodeState>>,
            )>())
            .saturating_add(
                transition
                    .slots
                    .iter()
                    .filter_map(|(_, state)| state.as_deref())
                    .fold(0usize, |total, state| {
                        total.saturating_add(state.accounted_state_bytes())
                    }),
            );
        let vnode_metadata = transition
            .resident_vnodes
            .capacity()
            .saturating_mul(std::mem::size_of::<u32>())
            .saturating_add(
                transition
                    .vnode_pending_holds
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Option<i64>>()),
            )
            .saturating_add(
                transition
                    .pending_hold_counts
                    .len()
                    .saturating_mul(PENDING_HOLD_ENTRY_CHARGE),
            );
        let assignment = transition.local_assignment.owners().len().saturating_mul(
            std::mem::size_of::<NodeId>().saturating_add(std::mem::size_of::<u64>()),
        );
        let peers = transition
            .cluster_peers
            .len()
            .saturating_mul(std::mem::size_of::<u64>());
        let channels = transition
            .peer_channels
            .iter()
            .map(BTreeMap::len)
            .sum::<usize>()
            .saturating_mul(
                std::mem::size_of::<(u64, TemporalPeerChannel)>()
                    .saturating_add(PENDING_HOLD_ENTRY_CHARGE),
            )
            .saturating_add(
                transition
                    .peer_channels
                    .iter()
                    .flat_map(BTreeMap::values)
                    .fold(0usize, |total, channel| {
                        total.saturating_add(
                            channel
                                .events
                                .capacity()
                                .saturating_mul(REMOTE_EVENT_CHARGE),
                        )
                    }),
            );
        std::mem::size_of::<PreparedTemporalJoinTransition>()
            .saturating_add(slots)
            .saturating_add(vnode_metadata)
            .saturating_add(assignment)
            .saturating_add(peers)
            .saturating_add(channels)
    }

    #[cfg(feature = "cluster")]
    fn derive_vnode_inventory<'a>(
        &self,
        assignment: &VnodeAssignmentSnapshot,
        self_id: NodeId,
        frontiers: [InputFrontier; 2],
        mut state_at: impl FnMut(u32) -> Option<&'a TemporalJoinVnodeState>,
    ) -> Result<TemporalVnodeInventory, DbError> {
        let mut inventory = TemporalVnodeInventory {
            resident_vnodes: Vec::with_capacity(self.vnode_states.len()),
            vnode_pending_holds: Vec::with_capacity(self.vnode_states.len()),
            pending_hold_counts: BTreeMap::new(),
            retained_state_bytes: 0,
            maintenance_pending: false,
        };
        for vnode in 0..self.vnode_count.get() {
            let state = state_at(vnode);
            let hold = state.and_then(TemporalJoinVnodeState::pending_watermark_hold);
            inventory.vnode_pending_holds.push(hold);
            if let Some(hold) = hold {
                let count = inventory
                    .pending_hold_counts
                    .get(&hold)
                    .copied()
                    .unwrap_or(0usize)
                    .checked_add(1)
                    .ok_or_else(|| self.accounting_error())?;
                inventory.pending_hold_counts.insert(hold, count);
            }
            let Some(state) = state else {
                continue;
            };
            if assignment.owners()[vnode as usize] != self_id
                || Self::vnode_state_frontiers(state) != frontiers
            {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] vnode {vnode} is outside its assignment state cut",
                    self.name
                )));
            }
            inventory.resident_vnodes.push(vnode);
            inventory.retained_state_bytes = inventory
                .retained_state_bytes
                .checked_add(state.accounted_state_bytes())
                .ok_or_else(|| self.accounting_error())?;
            inventory.maintenance_pending |=
                state.has_ready_probes() || state.has_history_gc_work();
        }
        Ok(inventory)
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

    fn refresh_vnode_pending_hold(&mut self, vnode: u32) -> Result<(), DbError> {
        let index = vnode as usize;
        let previous = self.vnode_pending_holds[index];
        let current = self.vnode_states[index]
            .as_ref()
            .and_then(|state| state.pending_watermark_hold());
        if previous == current {
            return Ok(());
        }
        let next_count = current
            .map(|hold| {
                self.pending_hold_counts
                    .get(&hold)
                    .copied()
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or_else(|| self.accounting_error())
            })
            .transpose()?;
        if let Some(hold) = previous {
            match self.pending_hold_counts.get(&hold).copied() {
                Some(1) => {
                    self.pending_hold_counts.remove(&hold);
                }
                Some(count) => {
                    self.pending_hold_counts.insert(hold, count - 1);
                }
                None => return Err(self.accounting_error()),
            }
        }
        if let (Some(hold), Some(count)) = (current, next_count) {
            self.pending_hold_counts.insert(hold, count);
        }
        self.vnode_pending_holds[index] = current;
        Ok(())
    }

    fn accounting_error(&self) -> DbError {
        DbError::Pipeline(format!(
            "temporal join [{}] retained-state accounting overflow",
            self.name
        ))
    }

    fn validate_inputs(&self, inputs: &[Vec<RecordBatch>]) -> Result<(), DbError> {
        if inputs.len() > 2 {
            return Err(DbError::InvalidOperation(format!(
                "temporal join [{}] accepts exactly two input ports",
                self.name
            )));
        }
        for batch in inputs.first().into_iter().flatten() {
            self.validate_side_batch(TemporalInputSide::Left, batch)?;
        }
        for batch in inputs.get(1).into_iter().flatten() {
            self.validate_side_batch(TemporalInputSide::Right, batch)?;
        }
        Ok(())
    }

    fn validate_side_batch(
        &self,
        side: TemporalInputSide,
        batch: &RecordBatch,
    ) -> Result<(), DbError> {
        let side_name = side.name();
        let schema = match side {
            TemporalInputSide::Left => {
                let mutations = source_mutations(batch).map_err(|error| {
                    DbError::SchemaMismatch(format!(
                        "temporal join [{}] left source metadata: {error}",
                        self.name
                    ))
                })?;
                if mutations.is_some() {
                    return Err(DbError::InvalidOperation(format!(
                        "temporal join [{}] left input must be append-only",
                        self.name
                    )));
                }
                batch.schema()
            }
            TemporalInputSide::Right => strip_source_mutations(batch)
                .map_err(|error| {
                    DbError::SchemaMismatch(format!(
                        "temporal join [{}] right source metadata: {error}",
                        self.name
                    ))
                })?
                .schema(),
        };
        let expected = match side {
            TemporalInputSide::Left => &self.left_schema,
            TemporalInputSide::Right => &self.right_schema,
        };
        if schema.as_ref() != expected.as_ref() {
            return Err(self.schema_error(side_name));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn validate_routed_side_batch(
        &self,
        side: TemporalInputSide,
        batch: &RecordBatch,
    ) -> Result<bool, DbError> {
        let (schema, mutation_stream) = match side {
            TemporalInputSide::Left => {
                let mutations = source_mutations_routed(batch).map_err(|error| {
                    DbError::SchemaMismatch(format!(
                        "temporal join [{}] routed left source metadata: {error}",
                        self.name
                    ))
                })?;
                if mutations.is_some() {
                    return Err(DbError::InvalidOperation(format!(
                        "temporal join [{}] left input must be append-only",
                        self.name
                    )));
                }
                (batch.schema(), false)
            }
            TemporalInputSide::Right => {
                let positioned = strip_source_mutations_routed(batch).map_err(|error| {
                    DbError::SchemaMismatch(format!(
                        "temporal join [{}] routed right source metadata: {error}",
                        self.name
                    ))
                })?;
                let mutation_stream = positioned.num_columns() != batch.num_columns();
                (positioned.schema(), mutation_stream)
            }
        };
        let expected = match side {
            TemporalInputSide::Left => &self.left_schema,
            TemporalInputSide::Right => &self.right_schema,
        };
        if schema.as_ref() != expected.as_ref() {
            return Err(self.schema_error(side.name()));
        }
        Ok(mutation_stream)
    }

    fn schema_error(&self, side: &str) -> DbError {
        DbError::SchemaMismatch(format!(
            "temporal join [{}] {side} batch does not match its declared positioned schema",
            self.name
        ))
    }

    fn encoded_route_keys(
        &self,
        side: TemporalInputSide,
        batch: &RecordBatch,
    ) -> Result<(Arc<Rows>, Vec<u32>), DbError> {
        let key_indices = match side {
            TemporalInputSide::Left => &self.left_key_indices,
            TemporalInputSide::Right => &self.right_key_indices,
        };
        let columns = key_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index)))
            .collect::<Vec<_>>();
        let keys = Arc::new(self.key_codec.encode_columns(&columns).map_err(|error| {
            DbError::Pipeline(format!(
                "temporal join [{}] {} key encoding: {error}",
                self.name,
                side.name()
            ))
        })?);
        let time_index = match side {
            TemporalInputSide::Left => self.left_time_index,
            TemporalInputSide::Right => self.right_time_index,
        };
        let times = TimestampMillisView::try_new(batch.column(time_index).as_ref(), side.name())?;
        let row_vnodes = keys
            .iter()
            .enumerate()
            .map(|(row, key)| {
                let _ = times.value(row, side.name())?;
                Ok(PartitionKeyCodecV1::vnode_for_encoded(
                    key.data(),
                    self.vnode_count,
                ))
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        Ok((keys, row_vnodes))
    }

    fn route_local_inputs(
        &self,
        inputs: &[Vec<RecordBatch>],
    ) -> Result<BTreeMap<u32, [Vec<RoutedTemporalBatch>; 2]>, DbError> {
        self.validate_inputs(inputs)?;
        let mut routed: BTreeMap<u32, [Vec<RoutedTemporalBatch>; 2]> = BTreeMap::new();
        for (side, batches) in [
            (
                TemporalInputSide::Left,
                inputs.first().map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
            (
                TemporalInputSide::Right,
                inputs.get(1).map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
        ] {
            for batch in batches.iter().filter(|batch| batch.num_rows() != 0) {
                let (keys, row_vnodes) = self.encoded_route_keys(side, batch)?;
                let plan = laminar_core::shuffle::route_checkpointed_batch(
                    batch,
                    &row_vnodes,
                    &self.local_assignment,
                    LOCAL_NODE_ID,
                )
                .map_err(|error| {
                    DbError::Pipeline(format!(
                        "temporal join [{}] local routing: {error}",
                        self.name
                    ))
                })?;
                if !plan.remote.is_empty() {
                    return Err(DbError::Pipeline(format!(
                        "temporal join [{}] local topology routed rows off-node",
                        self.name
                    )));
                }
                let port = usize::from(matches!(side, TemporalInputSide::Right));
                for route in plan.local {
                    routed.entry(route.vnode).or_default()[port].push(RoutedTemporalBatch {
                        batch: route.batch,
                        keys: Arc::clone(&keys),
                        source_rows: route.source_rows,
                    });
                }
            }
        }
        Ok(routed)
    }

    #[cfg(feature = "cluster")]
    fn effective_cluster_frontiers(
        &self,
        local: [InputFrontier; 2],
        applied: Option<&[BTreeMap<u64, InputFrontier>; 2]>,
        consumed: Option<&[BTreeMap<u64, usize>; 2]>,
    ) -> Result<[InputFrontier; 2], DbError> {
        let merge_side = |port: usize| -> Result<InputFrontier, DbError> {
            let peers = self.peer_channels[port].iter().map(|(peer, channel)| {
                let mut frontier = applied
                    .and_then(|frontiers| frontiers[port].get(peer).copied())
                    .unwrap_or(channel.applied);
                let consumed = consumed
                    .and_then(|counts| counts[port].get(peer).copied())
                    .unwrap_or(0);
                if channel.events.len() > consumed {
                    frontier.idle = false;
                    frontier.watermark =
                        max_watermark(frontier.watermark, self.frontiers[port].watermark);
                }
                frontier
            });
            let merged =
                merge_input_frontier_iter(std::iter::once(local[port]).chain(peers), i64::MIN);
            let side = if port == 0 { "left" } else { "right" };
            validate_frontier(self.frontiers[port], merged, side, &self.name)?;
            if self.pending_cluster_input.is_some() {
                return Ok(InputFrontier {
                    watermark: self.frontiers[port].watermark,
                    idle: false,
                });
            }
            Ok(merged)
        };
        Ok([merge_side(0)?, merge_side(1)?])
    }

    #[cfg(feature = "cluster")]
    fn plan_cluster_inputs(
        &self,
        inputs: &[Vec<RecordBatch>],
        frontiers: [InputFrontier; 2],
        config: &ClusterShuffleConfig,
        assignment: &VnodeAssignmentSnapshot,
        peers: &[u64],
    ) -> Result<ClusterInputPlan, DbError> {
        self.validate_inputs(inputs)?;
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
                    "temporal join [{}] received local input before its restored frontier was broadcast",
                    self.name
                )));
            }
            self.local_frontiers
        };
        for side in [TemporalInputSide::Left, TemporalInputSide::Right] {
            let port = side.port();
            validate_frontier(
                self.local_frontiers[port],
                frontiers[port],
                side.name(),
                &self.name,
            )?;
            if frontiers[port].idle
                && inputs
                    .get(port)
                    .is_some_and(|batches| batches.iter().any(|batch| batch.num_rows() != 0))
            {
                return Err(DbError::InvalidOperation(format!(
                    "temporal join [{}] received {} data from an idle local channel",
                    self.name,
                    side.name()
                )));
            }
        }
        let mut local_frontiers = frontiers;
        for side in [TemporalInputSide::Left, TemporalInputSide::Right] {
            let port = side.port();
            if self.local_frontiers[port].idle && !local_frontiers[port].idle {
                local_frontiers[port].watermark = max_watermark(
                    local_frontiers[port].watermark,
                    self.frontiers[port].watermark,
                );
            }
        }

        let mut routed: BTreeMap<u32, [Vec<RoutedTemporalBatch>; 2]> = BTreeMap::new();
        let mut remote_data: [BTreeMap<u64, Vec<laminar_core::shuffle::ShuffleMessage>>; 2] =
            [BTreeMap::new(), BTreeMap::new()];
        for side in [TemporalInputSide::Right, TemporalInputSide::Left] {
            let port = side.port();
            for batch in inputs
                .get(port)
                .into_iter()
                .flatten()
                .filter(|batch| batch.num_rows() != 0)
            {
                let accepted = if self.local_frontiers[port].idle {
                    self.frontiers[port]
                } else {
                    self.local_frontiers[port]
                };
                self.validate_batch_lateness(side, batch, accepted, false)?;
                let (keys, row_vnodes) = self.encoded_route_keys(side, batch)?;
                let plan = laminar_core::shuffle::route_checkpointed_batch(
                    batch,
                    &row_vnodes,
                    assignment,
                    config.self_id,
                )
                .map_err(|error| {
                    crate::operator::shuffle_routing_error(
                        &format!("temporal join [{}] {} routing", self.name, side.name()),
                        &error,
                    )
                })?;
                for route in plan.local {
                    routed.entry(route.vnode).or_default()[port].push(RoutedTemporalBatch {
                        batch: route.batch,
                        keys: Arc::clone(&keys),
                        source_rows: route.source_rows,
                    });
                }
                for route in plan.remote {
                    remote_data[port].entry(route.owner.0).or_default().push(
                        laminar_core::shuffle::ShuffleMessage::checkpointed_routed(
                            self.stage_for_side(side).to_owned(),
                            route.routed_vnodes,
                            route.batch,
                        ),
                    );
                }
            }
        }

        let mut outbound = Vec::new();
        for &peer in peers {
            for side in [TemporalInputSide::Right, TemporalInputSide::Left] {
                let port = side.port();
                let current = local_frontiers[port];
                let data = remote_data[port].remove(&peer);
                let has_data = data.as_ref().is_some_and(|messages| !messages.is_empty());
                if has_data && self.last_broadcasts[port].idle && !current.idle {
                    let previous = self.last_broadcasts[port];
                    outbound.push((
                        peer,
                        laminar_core::shuffle::ShuffleMessage::Frontier {
                            stage: self.stage_for_side(side).to_owned(),
                            watermark: previous.watermark,
                            idle: false,
                        },
                    ));
                }
                if let Some(messages) = data {
                    outbound.extend(messages.into_iter().map(|message| (peer, message)));
                }
                if has_data || self.last_broadcasts[port] != current {
                    outbound.push((
                        peer,
                        laminar_core::shuffle::ShuffleMessage::Frontier {
                            stage: self.stage_for_side(side).to_owned(),
                            watermark: current.watermark,
                            idle: current.idle,
                        },
                    ));
                }
            }
        }
        if remote_data.iter().any(|by_peer| !by_peer.is_empty()) {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] routed data outside its owner frontier roster",
                self.name
            )));
        }
        let effective_frontiers = self.effective_cluster_frontiers(local_frontiers, None, None)?;
        Ok(ClusterInputPlan {
            routed,
            outbound,
            local_frontiers,
            effective_frontiers,
        })
    }

    #[cfg(feature = "cluster")]
    fn validate_remote_batch_scope(
        &self,
        batch: &crate::operator::RetainedBatch,
        config: &ClusterShuffleConfig,
        assignment: &VnodeAssignmentSnapshot,
        peers: &[u64],
    ) -> Result<u64, DbError> {
        let peer = batch.peer().ok_or_else(|| {
            DbError::ShuffleTerminal(format!(
                "temporal join [{}] received unscoped shuffle data",
                self.name
            ))
        })?;
        if peers.binary_search(&peer).is_err()
            || batch.assignment_version() != Some(assignment.version())
            || batch.recovery_gen() != Some(config.receiver.recovery_gen())
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] received shuffle data from peer {peer} outside assignment {} recovery {}",
                self.name,
                assignment.version(),
                config.receiver.recovery_gen()
            )));
        }
        Ok(peer)
    }

    #[cfg(feature = "cluster")]
    fn validate_batch_lateness(
        &self,
        side: TemporalInputSide,
        batch: &RecordBatch,
        accepted: InputFrontier,
        reject_idle: bool,
    ) -> Result<(), DbError> {
        if reject_idle && accepted.idle {
            return Err(DbError::ShuffleTerminal(format!(
                "temporal join [{}] received {} data while peer channel was idle",
                self.name,
                side.name()
            )));
        }
        let Some(frontier) = accepted.watermark else {
            return Ok(());
        };
        let (time_index, lateness) = match side {
            TemporalInputSide::Left => (self.left_time_index, self.limits.left_allowed_lateness_ms),
            TemporalInputSide::Right => {
                (self.right_time_index, self.limits.right_allowed_lateness_ms)
            }
        };
        let times = TimestampMillisView::try_new(batch.column(time_index).as_ref(), side.name())?;
        for row in 0..times.len() {
            let event_time = times.value(row, side.name())?;
            let deadline = event_time.checked_add(lateness).ok_or_else(|| {
                DbError::Pipeline(format!(
                    "temporal join [{}] {} lateness deadline overflowed",
                    self.name,
                    side.name()
                ))
            })?;
            if deadline < frontier {
                return Err(DbError::Pipeline(format!(
                    "temporal join [{}] {} event at {} arrived behind its applied frontier {frontier} and allowed lateness",
                    self.name,
                    side.name(),
                    event_time
                )));
            }
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn remote_input_error(
        &self,
        side: TemporalInputSide,
        context: &str,
        error: &DbError,
    ) -> DbError {
        DbError::ShuffleTerminal(format!(
            "temporal join [{}] rejected {} shuffle {context}: {error}",
            self.name,
            side.name()
        ))
    }

    #[cfg(feature = "cluster")]
    fn prepare_remote_batch(
        &self,
        side: TemporalInputSide,
        retained: crate::operator::RetainedBatch,
        config: &ClusterShuffleConfig,
        assignment: &VnodeAssignmentSnapshot,
        peers: &[u64],
    ) -> Result<(u64, QueuedTemporalBatch), DbError> {
        let peer = self.validate_remote_batch_scope(&retained, config, assignment, peers)?;
        let accepted = self.peer_channels[side.port()]
            .get(&peer)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "temporal join [{}] has no {} channel for peer {peer}",
                    self.name,
                    side.name()
                ))
            })?
            .accepted;
        let batch =
            self.build_queued_batch(side, retained, accepted, assignment, config.self_id)?;
        let preflight_total = self
            .checked_accounted_state_bytes()?
            .checked_add(batch.charged_bytes)
            .and_then(|bytes| bytes.checked_add(REMOTE_EVENT_CHARGE))
            .ok_or_else(|| self.accounting_error())?;
        if preflight_total > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] ordered shuffle queue", self.name),
                accounted_bytes: preflight_total,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        Ok((peer, batch))
    }

    #[cfg(feature = "cluster")]
    fn build_queued_batch(
        &self,
        side: TemporalInputSide,
        retained: crate::operator::RetainedBatch,
        accepted: InputFrontier,
        assignment: &VnodeAssignmentSnapshot,
        self_id: NodeId,
    ) -> Result<QueuedTemporalBatch, DbError> {
        if retained.batch().num_rows() == 0 || retained.routed_vnodes().is_empty() {
            return Err(DbError::ShuffleTerminal(format!(
                "temporal join [{}] rejected empty {} shuffle data",
                self.name,
                side.name()
            )));
        }
        let mutation_stream = self
            .validate_routed_side_batch(side, retained.batch())
            .map_err(|error| self.remote_input_error(side, "schema", &error))?;
        self.validate_batch_lateness(side, retained.batch(), accepted, true)
            .map_err(|error| self.remote_input_error(side, "lateness", &error))?;
        let (keys, row_vnodes) = self
            .encoded_route_keys(side, retained.batch())
            .map_err(|error| self.remote_input_error(side, "partition key", &error))?;
        let mut actual = row_vnodes.clone();
        actual.sort_unstable();
        actual.dedup();
        if actual.iter().any(|vnode| {
            assignment
                .owners()
                .get(*vnode as usize)
                .is_none_or(|owner| *owner != self_id)
        }) {
            return Err(DbError::ShuffleTerminal(format!(
                "temporal join [{}] received {} data outside this node's vnode ownership",
                self.name,
                side.name()
            )));
        }
        if actual.as_slice() != retained.routed_vnodes() {
            return Err(DbError::ShuffleTerminal(format!(
                "temporal join [{}] {} shuffle vnode metadata {:?} does not match decoded rows {actual:?}",
                self.name,
                side.name(),
                retained.routed_vnodes()
            )));
        }
        let charged_bytes = retained
            .heap_bytes()
            .and_then(|bytes| bytes.checked_add(RETAINED_BATCH_ARC_CHARGE))
            .and_then(|bytes| bytes.checked_add(keys.size()))
            .and_then(|bytes| {
                row_vnodes
                    .capacity()
                    .checked_mul(std::mem::size_of::<u32>())
                    .and_then(|vnodes| bytes.checked_add(vnodes))
            })
            .ok_or_else(|| self.accounting_error())?;
        Ok(QueuedTemporalBatch {
            retained: Arc::new(retained),
            keys,
            row_vnodes,
            charged_bytes,
            mutation_stream,
        })
    }

    #[cfg(feature = "cluster")]
    fn has_remote_events(&self) -> bool {
        self.queued_remote_events != 0
    }

    #[cfg(feature = "cluster")]
    fn build_remote_drain_plan(
        &self,
        assignment: &VnodeAssignmentSnapshot,
        self_id: NodeId,
    ) -> Result<RemoteDrainPlan, DbError> {
        let mut plan = RemoteDrainPlan {
            routed: BTreeMap::new(),
            applied: [BTreeMap::new(), BTreeMap::new()],
            consumed: [BTreeMap::new(), BTreeMap::new()],
            cursors: self.remote_peer_cursors,
            released_bytes: 0,
        };
        for side in [TemporalInputSide::Right, TemporalInputSide::Left] {
            let port = side.port();
            let peers = self.cluster_peers.as_ref();
            if peers.is_empty() {
                continue;
            }
            let mut index = self.remote_peer_cursors[port].map_or(0, |cursor| {
                let next = peers.partition_point(|peer| *peer <= cursor);
                if next == peers.len() {
                    0
                } else {
                    next
                }
            });
            let mut empty_visits = 0usize;
            let mut budget = REMOTE_EVENT_BUDGET_PER_SIDE;
            let mut admitted_bytes = 0usize;
            let mut admitted_rows = 0usize;
            let mut admitted_events = 0usize;
            while budget != 0 && empty_visits < peers.len() {
                let peer = peers[index];
                index = (index + 1) % peers.len();
                let offset = plan.consumed[port].get(&peer).copied().unwrap_or(0);
                let channel = &self.peer_channels[port][&peer];
                let Some(event) = channel.events.get(offset) else {
                    empty_visits += 1;
                    continue;
                };
                let event_bytes = event.drain_bytes().ok_or_else(|| self.accounting_error())?;
                let next_bytes = admitted_bytes
                    .checked_add(event_bytes)
                    .ok_or_else(|| self.accounting_error())?;
                let next_rows = admitted_rows
                    .checked_add(event.rows())
                    .ok_or_else(|| self.accounting_error())?;
                if admitted_events != 0
                    && (next_bytes > REMOTE_DRAIN_BYTE_BUDGET_PER_SIDE
                        || next_rows > REMOTE_DRAIN_ROW_BUDGET_PER_SIDE)
                {
                    break;
                }
                empty_visits = 0;
                match &event.payload {
                    TemporalRemoteEventPayload::Data(batch) => {
                        let routes = laminar_core::shuffle::route_checkpointed_batch(
                            batch.retained.batch(),
                            &batch.row_vnodes,
                            assignment,
                            self_id,
                        )
                        .map_err(|error| {
                            crate::operator::shuffle_routing_error(
                                &format!(
                                    "temporal join [{}] queued {} routing",
                                    self.name,
                                    side.name()
                                ),
                                &error,
                            )
                        })?;
                        if !routes.remote.is_empty() {
                            return Err(DbError::Checkpoint(format!(
                                "temporal join [{}] queued {} data is no longer locally owned",
                                self.name,
                                side.name()
                            )));
                        }
                        for route in routes.local {
                            plan.routed.entry(route.vnode).or_default()[port].push(
                                RoutedTemporalBatch {
                                    batch: route.batch,
                                    keys: Arc::clone(&batch.keys),
                                    source_rows: route.source_rows,
                                },
                            );
                        }
                    }
                    TemporalRemoteEventPayload::Frontier(frontier) => {
                        let previous = plan.applied[port]
                            .get(&peer)
                            .copied()
                            .unwrap_or(channel.applied);
                        validate_frontier(previous, *frontier, side.name(), &self.name)?;
                        plan.applied[port].insert(peer, *frontier);
                    }
                }
                admitted_bytes = next_bytes;
                admitted_rows = next_rows;
                admitted_events += 1;
                plan.released_bytes = plan
                    .released_bytes
                    .checked_add(event.payload_bytes())
                    .ok_or_else(|| self.accounting_error())?;
                plan.consumed[port].insert(peer, offset + 1);
                plan.cursors[port] = Some(peer);
                budget -= 1;
            }
        }
        Ok(plan)
    }

    #[cfg(feature = "cluster")]
    fn commit_remote_drain(&mut self, plan: &RemoteDrainPlan) {
        for port in 0..2 {
            for (&peer, &count) in &plan.consumed[port] {
                let channel = self.peer_channels[port]
                    .get_mut(&peer)
                    .expect("planned temporal peer channel");
                for _ in 0..count {
                    channel
                        .events
                        .pop_front()
                        .expect("planned temporal remote event");
                }
            }
            for (&peer, &frontier) in &plan.applied[port] {
                self.peer_channels[port]
                    .get_mut(&peer)
                    .expect("planned temporal peer frontier")
                    .applied = frontier;
            }
        }
        self.remote_peer_cursors = plan.cursors;
        self.queued_shuffle_bytes = self
            .queued_shuffle_bytes
            .checked_sub(plan.released_bytes)
            .expect("planned temporal queue accounting");
        let released_events = plan
            .consumed
            .iter()
            .flat_map(BTreeMap::values)
            .sum::<usize>();
        self.queued_remote_events = self
            .queued_remote_events
            .checked_sub(released_events)
            .expect("planned temporal event accounting");
    }

    #[cfg(feature = "cluster")]
    fn normalize_remote_frontier(
        &self,
        side: TemporalInputSide,
        mut next: InputFrontier,
    ) -> InputFrontier {
        let port = side.port();
        let floor = self
            .pending_frontiers
            .map_or(self.frontiers[port], |pending| pending[port]);
        next.watermark = max_watermark(next.watermark, floor.watermark);
        next
    }

    fn mark_vnode_dirty(&mut self, vnode: u32) {
        let dirty = self
            .dirty_vnodes
            .get_mut(vnode as usize)
            .expect("temporal dirty vnode must be inside the configured topology");
        if !*dirty {
            *dirty = true;
            debug_assert!(self.dirty_vnode_roster.len() < self.dirty_vnode_roster.capacity());
            self.dirty_vnode_roster.push(vnode);
        }
    }

    fn clear_dirty_vnodes(&mut self) {
        for vnode in self.dirty_vnode_roster.drain(..) {
            let dirty = self
                .dirty_vnodes
                .get_mut(vnode as usize)
                .expect("temporal dirty vnode must remain inside the configured topology");
            debug_assert!(*dirty);
            *dirty = false;
        }
    }

    fn prepare_vnode(&mut self, vnode: u32, accounted_total: &mut usize) -> Result<usize, DbError> {
        let index = vnode as usize;
        if self.vnode_states[index].is_none() {
            let shard_limit = self
                .max_managed_state_bytes
                .checked_sub(*accounted_total)
                .and_then(|limit| limit.checked_sub(PENDING_HOLD_ENTRY_CHARGE))
                .filter(|limit| *limit != 0)
                .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                    context: format!("temporal join [{}] vnode {vnode}", self.name),
                    accounted_bytes: *accounted_total,
                    limit_bytes: self.max_managed_state_bytes,
                })?;
            let config = self.state_config(vnode, shard_limit);
            let mut state = TemporalJoinVnodeState::try_new(
                Arc::clone(&self.left_schema),
                Arc::clone(&self.right_schema),
                config,
            )?;
            state.advance_left_frontier(self.frontiers[0].watermark, self.frontiers[0].idle)?;
            state.advance_right_frontier(self.frontiers[1].watermark, self.frontiers[1].idle)?;
            let state_bytes = state.accounted_state_bytes();
            let retained_state_bytes = self
                .retained_state_bytes
                .checked_add(state_bytes)
                .ok_or_else(|| self.accounting_error())?;
            let next_total = accounted_total
                .checked_add(state_bytes)
                .ok_or_else(|| self.accounting_error())?;
            self.retained_state_bytes = retained_state_bytes;
            *accounted_total = next_total;
            self.vnode_states[index] = Some(Box::new(state));
            self.add_resident_vnode(vnode);
            self.mark_vnode_dirty(vnode);
        }
        let current = self.vnode_states[index]
            .as_ref()
            .expect("temporal vnode state initialized")
            .accounted_state_bytes();
        let other = accounted_total
            .checked_sub(current)
            .ok_or_else(|| self.accounting_error())?;
        let shard_limit = self
            .max_managed_state_bytes
            .checked_sub(other)
            .and_then(|limit| limit.checked_sub(PENDING_HOLD_ENTRY_CHARGE))
            .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] vnode {vnode}", self.name),
                accounted_bytes: *accounted_total,
                limit_bytes: self.max_managed_state_bytes,
            })?;
        let state = self.vnode_states[index]
            .as_mut()
            .expect("temporal vnode state initialized");
        state.set_retained_byte_limit(shard_limit)?;
        Ok(current)
    }

    fn refresh_vnode_accounting(
        &mut self,
        vnode: u32,
        previous: usize,
        accounted_total: &mut usize,
    ) -> Result<(), DbError> {
        let current = self.vnode_states[vnode as usize]
            .as_ref()
            .expect("temporal vnode state initialized")
            .accounted_state_bytes();
        let retained_state_bytes = self
            .retained_state_bytes
            .checked_sub(previous)
            .and_then(|bytes| bytes.checked_add(current))
            .ok_or_else(|| self.accounting_error())?;
        self.retained_state_bytes = retained_state_bytes;
        self.refresh_vnode_pending_hold(vnode)?;
        let next_total = self.checked_accounted_state_bytes()?;
        if next_total > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] vnode {vnode}", self.name),
                accounted_bytes: next_total,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        *accounted_total = next_total;
        Ok(())
    }

    fn apply_right_batches(
        &mut self,
        vnode: u32,
        batches: &[RoutedTemporalBatch],
        accounted_total: &mut usize,
    ) -> Result<bool, DbError> {
        if batches.is_empty() {
            return Ok(false);
        }
        let mut applied = false;
        for routed in batches {
            let operations = source_mutations_routed(&routed.batch).map_err(|error| {
                DbError::SchemaMismatch(format!(
                    "temporal join [{}] routed right mutations: {error}",
                    self.name
                ))
            });
            let operations = match operations {
                Ok(operations) => operations,
                Err(error) => return Err(self.after_apply_error(applied, vnode, error)),
            };
            let positioned = strip_source_mutations_routed(&routed.batch).map_err(|error| {
                DbError::SchemaMismatch(format!(
                    "temporal join [{}] routed right mutations: {error}",
                    self.name
                ))
            });
            let positioned = match positioned {
                Ok(positioned) => positioned,
                Err(error) => return Err(self.after_apply_error(applied, vnode, error)),
            };
            let previous = self.prepare_vnode(vnode, accounted_total)?;
            let result = self.vnode_states[vnode as usize]
                .as_mut()
                .expect("temporal vnode state initialized")
                .apply_right_batch_routed(
                    &positioned,
                    operations,
                    &routed.keys,
                    &routed.source_rows,
                );
            let stats = match result {
                Ok(stats) => stats,
                Err(error) => return Err(self.after_apply_error(applied, vnode, error)),
            };
            if stats.duplicates != positioned.num_rows() {
                applied = true;
                self.mark_vnode_dirty(vnode);
            }
            self.refresh_vnode_accounting(vnode, previous, accounted_total)
                .map_err(|error| self.after_apply_error(applied, vnode, error))?;
        }
        Ok(applied)
    }

    fn apply_left_batches(
        &mut self,
        vnode: u32,
        batches: &[RoutedTemporalBatch],
        accounted_total: &mut usize,
    ) -> Result<(bool, Vec<RecordBatch>), DbError> {
        if batches.is_empty() {
            return Ok((false, Vec::new()));
        }
        let mut output = Vec::new();
        let mut applied = false;
        for routed in batches {
            let previous = self.prepare_vnode(vnode, accounted_total)?;
            let result = self.vnode_states[vnode as usize]
                .as_mut()
                .expect("temporal vnode state initialized")
                .probe_left_batch_routed(&routed.batch, &routed.keys, &routed.source_rows);
            let result = match result {
                Ok(result) => result,
                Err(error) => return Err(self.after_apply_error(applied, vnode, error)),
            };
            applied = true;
            if result.num_rows() != 0 {
                output.push(result);
            }
            self.mark_vnode_dirty(vnode);
            self.refresh_vnode_accounting(vnode, previous, accounted_total)
                .map_err(|error| self.after_apply_error(applied, vnode, error))?;
        }
        Ok((true, output))
    }

    fn apply_frontiers(
        &mut self,
        next: [InputFrontier; 2],
        accounted_total: &mut usize,
    ) -> Result<bool, DbError> {
        validate_frontier(self.frontiers[0], next[0], "left", &self.name)?;
        validate_frontier(self.frontiers[1], next[1], "right", &self.name)?;
        if self.pending_frontiers.is_none() && self.frontiers == next {
            return Ok(false);
        }
        if let Some(pending) = self.pending_frontiers {
            validate_frontier(pending[0], next[0], "left", &self.name)?;
            validate_frontier(pending[1], next[1], "right", &self.name)?;
        } else {
            self.pending_frontiers = Some(next);
            self.frontier_cursor = 0;
            self.frontier_remaining = self.resident_vnodes.len();
            self.frontier_has_work = false;
        }
        let target = self.pending_frontiers.expect("staged temporal frontiers");
        let resident_count = self.resident_vnodes.len();
        if self.frontier_remaining == 0 {
            self.frontiers = target;
            self.pending_frontiers = None;
            return Ok(true);
        }
        let visit_limit = self
            .limits
            .maintenance_vnode_budget
            .get()
            .min(self.frontier_remaining)
            .min(resident_count);
        let cursor = u32::try_from(self.frontier_cursor).map_err(|_| {
            DbError::Pipeline(format!(
                "temporal join [{}] frontier cursor exceeds u32",
                self.name
            ))
        })?;
        let mut roster_index = self
            .resident_vnodes
            .partition_point(|resident| *resident < cursor);
        if roster_index == resident_count {
            roster_index = 0;
        }
        let mut applied = false;
        for _ in 0..visit_limit {
            let vnode = self.resident_vnodes[roster_index];
            roster_index += 1;
            if roster_index == resident_count {
                roster_index = 0;
            }
            let previous = match self.prepare_vnode(vnode, accounted_total) {
                Ok(previous) => previous,
                Err(error) => {
                    return Err(self.after_apply_error(applied, vnode, error));
                }
            };
            let left = self.vnode_states[vnode as usize]
                .as_mut()
                .expect("resident state")
                .advance_left_frontier(target[0].watermark, target[0].idle);
            if let Err(error) = left {
                return Err(self.after_apply_error(applied, vnode, error));
            }
            applied = true;
            self.mark_vnode_dirty(vnode);
            let right = self.vnode_states[vnode as usize]
                .as_mut()
                .expect("resident state")
                .advance_right_frontier(target[1].watermark, target[1].idle);
            if let Err(error) = right {
                return Err(self.after_apply_error(applied, vnode, error));
            }
            let state = self.vnode_states[vnode as usize]
                .as_ref()
                .expect("resident state");
            let has_work = state.has_ready_probes() || state.has_history_gc_work();
            if let Err(error) = self.refresh_vnode_accounting(vnode, previous, accounted_total) {
                return Err(self.after_apply_error(applied, vnode, error));
            }
            self.frontier_has_work |= has_work;
            self.frontier_cursor = (vnode as usize + 1) % self.vnode_states.len();
            self.frontier_remaining -= 1;
        }
        if self.frontier_remaining == 0 {
            self.frontiers = target;
            self.pending_frontiers = None;
            self.maintenance_pending = self.frontier_has_work;
            self.maintenance_remaining = if self.frontier_has_work {
                resident_count
            } else {
                0
            };
            self.maintenance_rescan = false;
            self.frontier_has_work = false;
        }
        Ok(true)
    }

    fn drain_maintenance(
        &mut self,
        accounted_total: &mut usize,
    ) -> Result<(bool, Vec<RecordBatch>), DbError> {
        let resident_count = self.resident_vnodes.len();
        if resident_count == 0 || !self.maintenance_pending {
            self.maintenance_pending = false;
            self.maintenance_remaining = 0;
            self.maintenance_rescan = false;
            return Ok((false, Vec::new()));
        }
        if self.maintenance_remaining == 0 {
            if self.maintenance_rescan {
                self.maintenance_remaining = resident_count;
                self.maintenance_rescan = false;
            } else {
                self.maintenance_pending = false;
                return Ok((false, Vec::new()));
            }
        }
        let mut ready = self.limits.ready_probe_budget.get();
        let mut gc = self.limits.history_gc_budget.get();
        let visit_limit = self
            .limits
            .maintenance_vnode_budget
            .get()
            .min(self.maintenance_remaining)
            .min(resident_count);
        let cursor = u32::try_from(self.maintenance_cursor).map_err(|_| {
            DbError::Pipeline(format!(
                "temporal join [{}] maintenance cursor exceeds u32",
                self.name
            ))
        })?;
        let mut roster_index = self
            .resident_vnodes
            .partition_point(|resident| *resident < cursor);
        if roster_index == resident_count {
            roster_index = 0;
        }
        let mut visited = 0usize;
        let mut changed = false;
        let mut applied = false;
        let mut output = Vec::new();
        while visited < visit_limit && ready != 0 && gc != 0 {
            let vnode = self.resident_vnodes[roster_index];
            roster_index += 1;
            if roster_index == resident_count {
                roster_index = 0;
            }
            let previous = match self.prepare_vnode(vnode, accounted_total) {
                Ok(previous) => previous,
                Err(error) => {
                    return Err(self.after_apply_error(applied, vnode, error));
                }
            };
            let mut vnode_changed = false;
            let mut vnode_has_more = false;
            if ready != 0 {
                let drained = self.vnode_states[vnode as usize]
                    .as_mut()
                    .expect("resident state")
                    .drain_ready_probes(NonZeroUsize::new(ready).expect("positive ready budget"));
                let drained = match drained {
                    Ok(drained) => drained,
                    Err(error) => {
                        return Err(self.after_apply_error(applied, vnode, error));
                    }
                };
                ready -= drained.drained_probes;
                vnode_has_more |= drained.has_more;
                if drained.drained_probes != 0 {
                    vnode_changed = true;
                    applied = true;
                    self.mark_vnode_dirty(vnode);
                    if drained.output.num_rows() != 0 {
                        output.push(drained.output);
                    }
                }
            } else if self.vnode_states[vnode as usize]
                .as_ref()
                .expect("resident state")
                .has_ready_probes()
            {
                vnode_has_more = true;
            }
            if gc != 0 {
                let had_gc_work = self.vnode_states[vnode as usize]
                    .as_ref()
                    .expect("resident state")
                    .has_history_gc_work();
                let drained = self.vnode_states[vnode as usize]
                    .as_mut()
                    .expect("resident state")
                    .drain_history_gc(NonZeroUsize::new(gc).expect("positive GC budget"));
                let drained = match drained {
                    Ok(drained) => drained,
                    Err(error) => {
                        return Err(self.after_apply_error(applied, vnode, error));
                    }
                };
                gc -= drained.steps;
                vnode_has_more |= drained.has_more;
                if drained.steps != 0 || had_gc_work {
                    vnode_changed = true;
                    applied = true;
                    self.mark_vnode_dirty(vnode);
                }
            } else if self.vnode_states[vnode as usize]
                .as_ref()
                .expect("resident state")
                .has_history_gc_work()
            {
                vnode_has_more = true;
            }
            if vnode_changed {
                changed = true;
            }
            if let Err(error) = self.refresh_vnode_accounting(vnode, previous, accounted_total) {
                return Err(self.after_apply_error(applied, vnode, error));
            }
            self.maintenance_rescan |= vnode_has_more;
            self.maintenance_cursor = (vnode as usize + 1) % self.vnode_states.len();
            self.maintenance_remaining -= 1;
            visited += 1;
        }
        if self.maintenance_remaining == 0 {
            if self.maintenance_rescan {
                self.maintenance_remaining = resident_count;
                self.maintenance_rescan = false;
            } else {
                self.maintenance_pending = false;
            }
        }
        Ok((changed, output))
    }

    fn output_watermark_ceiling_for(&self, left: InputFrontier) -> i64 {
        let Some(watermark) = left.watermark else {
            return i64::MIN;
        };
        let left_floor = watermark.saturating_sub(self.limits.left_allowed_lateness_ms);
        left_floor.min(left_floor.saturating_add(self.minimum_probe_offset))
    }

    fn output_watermark_ceiling(&self) -> i64 {
        self.output_watermark_ceiling_for(self.frontiers[0])
    }

    fn validate_published_output_frontier(
        &self,
        frontiers: [InputFrontier; 2],
        published: Option<InputFrontier>,
    ) -> Result<(), DbError> {
        let maximum = merge_input_frontiers(&frontiers, i64::MIN)
            .with_watermark_ceiling(Some(self.output_watermark_ceiling_for(frontiers[0])))
            .watermark;
        if published
            .and_then(|frontier| frontier.watermark)
            .is_some_and(|watermark| maximum.is_none_or(|maximum| watermark > maximum))
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] published output frontier exceeds its input ceiling",
                self.name
            )));
        }
        Ok(())
    }

    fn pending_output_hold(&self) -> Option<i64> {
        let pending_hold = self
            .pending_hold_counts
            .first_key_value()
            .map(|(hold, _)| *hold);
        let staged_hold = self.pending_frontiers.map(|_| {
            self.published_output_frontier
                .and_then(|frontier| frontier.watermark)
                .unwrap_or(i64::MIN)
        });
        #[cfg(feature = "cluster")]
        let remote_hold =
            (self.pending_cluster_input.is_some() || self.has_remote_events()).then(|| {
                self.published_output_frontier
                    .and_then(|frontier| frontier.watermark)
                    .unwrap_or(i64::MIN)
            });
        #[cfg(not(feature = "cluster"))]
        let remote_hold = None;
        pending_hold
            .into_iter()
            .chain(staged_hold)
            .chain(remote_hold)
            .min()
    }

    fn derive_output_frontier(&self, input: InputFrontier) -> InputFrontier {
        let mut output = input
            .with_watermark_ceiling(Some(self.output_watermark_ceiling()))
            .held_at(self.pending_output_hold());
        if self.maintenance_pending {
            output.idle = false;
        }
        output
    }

    fn record_published_output_frontier(&mut self, input_frontiers: &[InputFrontier]) {
        let output = self.derive_output_frontier(merge_input_frontiers(input_frontiers, i64::MIN));
        if output == InputFrontier::default() && self.published_output_frontier.is_none() {
            return;
        }
        let watermark = match (
            self.published_output_frontier
                .and_then(|frontier| frontier.watermark),
            output.watermark,
        ) {
            (Some(previous), Some(current)) => Some(previous.max(current)),
            (Some(previous), None) => Some(previous),
            (None, current) => current,
        };
        self.published_output_frontier = Some(InputFrontier {
            watermark,
            idle: output.idle,
        });
    }

    fn process_common(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: [InputFrontier; 2],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if (self.pending_frontiers.is_some() || self.maintenance_pending)
            && inputs.iter().any(|batches| !batches.is_empty())
        {
            return Err(DbError::InvalidOperation(format!(
                "temporal join [{}] received input while bounded work was pending",
                self.name
            )));
        }
        let routed = self.route_local_inputs(inputs)?;
        self.execute_routed(&routed, frontiers)
    }

    fn execute_routed(
        &mut self,
        routed: &BTreeMap<u32, [Vec<RoutedTemporalBatch>; 2]>,
        frontiers: [InputFrontier; 2],
    ) -> Result<Vec<RecordBatch>, DbError> {
        validate_frontier(self.frontiers[0], frontiers[0], "left", &self.name)?;
        validate_frontier(self.frontiers[1], frontiers[1], "right", &self.name)?;
        let mut accounted = self.checked_accounted_state_bytes()?;
        if accounted > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}]", self.name),
                accounted_bytes: accounted,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        let mut applied = false;
        for (&vnode, sides) in routed {
            match self.apply_right_batches(vnode, &sides[1], &mut accounted) {
                Ok(changed) => applied |= changed,
                Err(error) => return Err(self.after_apply_error(applied, vnode, error)),
            }
        }
        let mut output = Vec::new();
        for (&vnode, sides) in routed {
            match self.apply_left_batches(vnode, &sides[0], &mut accounted) {
                Ok((changed, batches)) => {
                    applied |= changed;
                    output.extend(batches);
                }
                Err(error) => return Err(self.after_apply_error(applied, vnode, error)),
            }
        }
        if self.pending_frontiers.is_some() || !self.maintenance_pending {
            match self.apply_frontiers(frontiers, &mut accounted) {
                Ok(changed) => applied |= changed,
                Err(error) => return Err(self.after_apply_error(applied, 0, error)),
            }
        }
        if self.pending_frontiers.is_none() && self.maintenance_pending {
            match self.drain_maintenance(&mut accounted) {
                Ok((_, batches)) => output.extend(batches),
                Err(error) => return Err(self.after_apply_error(applied, 0, error)),
            }
        }
        self.record_published_output_frontier(&frontiers);
        Ok(output)
    }

    #[cfg(feature = "cluster")]
    fn outbound_finalize_error(&self, error: DbError) -> DbError {
        if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
            return error;
        }
        DbError::ShufflePartialSend(format!(
            "temporal join [{}] failed after outbound shuffle admission: {error}",
            self.name
        ))
    }

    #[cfg(feature = "cluster")]
    fn remote_replay_error(&self, error: DbError) -> DbError {
        if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
            error
        } else {
            DbError::Checkpoint(format!(
                "temporal join [{}] ordered shuffle replay requires recovery: {error}",
                self.name
            ))
        }
    }

    #[cfg(feature = "cluster")]
    async fn drain_remote_events(
        &mut self,
        config: &ClusterShuffleConfig,
        assignment: &VnodeAssignmentSnapshot,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let plan = self
            .build_remote_drain_plan(assignment, config.self_id)
            .map_err(|error| self.remote_replay_error(error))?;
        let effective = self
            .effective_cluster_frontiers(
                self.local_frontiers,
                Some(&plan.applied),
                Some(&plan.consumed),
            )
            .map_err(|error| self.remote_replay_error(error))?;
        self.commit_remote_drain(&plan);
        let output = self
            .execute_routed(&plan.routed, effective)
            .map_err(|error| self.remote_replay_error(error))?;
        let output = self
            .project_output(output)
            .await
            .map_err(|error| self.remote_replay_error(error))?;
        self.record_published_output_frontier(&effective);
        Ok(output)
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
            .expect("temporal send plan must be installed before it starts");
        debug_assert!(pending.send.is_none());
        let outbound = pending
            .outbound
            .take()
            .expect("idle temporal send plan must retain its outbound cut");
        let sender = Arc::clone(&config.sender);
        let wake = config.receiver.work_ready_notify();
        let context = format!("temporal join [{}] shuffle", self.name);
        pending.send = Some(tokio::spawn(async move {
            let result = crate::operator::send_shuffle_plan_retaining(
                &sender,
                assignment_version,
                outbound,
                &context,
            )
            .await;
            if !matches!(&result.0, Err(error) if error.is_shuffle_not_ready()) {
                wake.notify_one();
            }
            result
        }));
    }

    #[cfg(feature = "cluster")]
    async fn finish_pending_cluster_input(&mut self) -> Result<PendingTemporalCompletion, DbError> {
        let finished = self
            .pending_cluster_input
            .as_ref()
            .and_then(|pending| pending.send.as_ref())
            .is_some_and(tokio::task::JoinHandle::is_finished);
        if !finished {
            return Ok(PendingTemporalCompletion::Waiting);
        }
        let mut pending = self
            .pending_cluster_input
            .take()
            .expect("finished temporal send plan");
        let send = pending.send.take().expect("pending temporal send task");
        let (result, outbound) = send.await.map_err(|error| {
            DbError::ShufflePartialSend(format!(
                "temporal join [{}] send task ended without a delivery outcome: {error}",
                self.name
            ))
        })?;
        if let Err(error) = result {
            if error.is_shuffle_not_ready() {
                pending.outbound = Some(outbound.ok_or_else(|| {
                    DbError::ShufflePartialSend(format!(
                        "temporal join [{}] safe send failure lost its retry plan",
                        self.name
                    ))
                })?);
                self.pending_cluster_input = Some(pending);
                return Ok(PendingTemporalCompletion::RetryLater);
            }
            return Err(error);
        }
        debug_assert!(outbound.is_none());

        let effective = self
            .effective_cluster_frontiers(pending.local_frontiers, None, None)
            .map_err(|error| self.outbound_finalize_error(error))?;
        let routed = std::mem::take(&mut pending.routed);
        let local_frontiers = pending.local_frontiers;
        drop(pending);
        let output = self
            .execute_routed(&routed, effective)
            .map_err(|error| self.outbound_finalize_error(error))?;
        let output = self
            .project_output(output)
            .await
            .map_err(|error| self.outbound_finalize_error(self.post_projection_error(error)))?;
        self.local_frontiers = local_frontiers;
        self.last_broadcasts = local_frontiers;
        Ok(PendingTemporalCompletion::Applied(output))
    }

    #[cfg(feature = "cluster")]
    async fn process_cluster(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: [InputFrontier; 2],
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
        let mut processed_deferred_work = false;
        if self.pending_frontiers.is_some() || self.maintenance_pending {
            if inputs.iter().any(|batches| !batches.is_empty()) {
                let error = DbError::InvalidOperation(format!(
                    "temporal join [{}] received input while bounded work was pending",
                    self.name
                ));
                return Err(if self.pending_cluster_input.is_some() {
                    self.outbound_finalize_error(error)
                } else {
                    error
                });
            }
            let target = self.pending_frontiers.unwrap_or(self.frontiers);
            deferred_output = self
                .process_and_project(&[], target)
                .await
                .map_err(|error| self.remote_replay_error(error))?;
            processed_deferred_work = true;
            if self.pending_frontiers.is_some() || self.maintenance_pending {
                return Ok(deferred_output);
            }
        }
        if self.has_remote_events() {
            if inputs.iter().any(|batches| !batches.is_empty()) {
                let error = DbError::InvalidOperation(format!(
                    "temporal join [{}] received local input while ordered shuffle replay was pending",
                    self.name
                ));
                return Err(if self.pending_cluster_input.is_some() {
                    self.outbound_finalize_error(error)
                } else {
                    error
                });
            }
            deferred_output.extend(self.drain_remote_events(&config, &assignment).await?);
            processed_deferred_work = true;
            if self.pending_frontiers.is_some() || self.maintenance_pending {
                return Ok(deferred_output);
            }
        }
        let completion = self.finish_pending_cluster_input().await.map_err(|error| {
            if processed_deferred_work {
                self.remote_replay_error(error)
            } else {
                error
            }
        })?;
        match completion {
            PendingTemporalCompletion::Applied(output) => {
                deferred_output.extend(output);
                return Ok(deferred_output);
            }
            PendingTemporalCompletion::Waiting | PendingTemporalCompletion::RetryLater => {}
        }
        if self.pending_cluster_input.is_some() {
            if inputs.iter().any(|batches| !batches.is_empty()) {
                return Err(
                    self.outbound_finalize_error(DbError::InvalidOperation(format!(
                        "temporal join [{}] received local input while a shuffle send was pending",
                        self.name
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
        if processed_deferred_work {
            return Ok(deferred_output);
        }

        let plan = self.plan_cluster_inputs(inputs, frontiers, &config, &assignment, &peers)?;
        if !plan.outbound.is_empty() {
            let accounted_bytes = self.cluster_input_plan_bytes(&plan)?;
            let total = self
                .checked_accounted_state_bytes()?
                .checked_add(accounted_bytes)
                .ok_or_else(|| self.accounting_error())?;
            if total > self.max_managed_state_bytes {
                return Err(DbError::ManagedStateBudgetExceeded {
                    context: format!("temporal join [{}] pending shuffle send", self.name),
                    accounted_bytes: total,
                    limit_bytes: self.max_managed_state_bytes,
                });
            }
            let ClusterInputPlan {
                routed,
                outbound,
                local_frontiers,
                effective_frontiers: _,
            } = plan;
            self.pending_cluster_input = Some(PendingTemporalClusterInput {
                routed,
                outbound: Some(outbound),
                local_frontiers,
                send: None,
                accounted_bytes,
            });
            self.start_pending_cluster_send(&config, assignment.version());
            return Ok(Vec::new());
        }
        let output = self.execute_routed(&plan.routed, plan.effective_frontiers)?;
        let output = self
            .project_output(output)
            .await
            .map_err(|error| self.post_projection_error(error))?;
        self.local_frontiers = plan.local_frontiers;
        self.last_broadcasts = plan.local_frontiers;
        Ok(output)
    }

    async fn project_output(
        &mut self,
        join_result: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.projection.apply(join_result).await
    }

    async fn process_and_project(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: [InputFrontier; 2],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let output = self.process_common(inputs, frontiers)?;
        self.project_output(output)
            .await
            .map_err(|error| self.post_projection_error(error))
    }

    fn post_projection_error(&self, error: DbError) -> DbError {
        if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
            error
        } else {
            DbError::StatefulOperatorPartialApply(format!(
                "temporal join [{}] admitted state before post-projection failed: {error}",
                self.name
            ))
        }
    }

    fn after_apply_error(&self, applied: bool, vnode: u32, error: DbError) -> DbError {
        if !applied || error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
            return error;
        }
        DbError::StatefulOperatorPartialApply(format!(
            "temporal join [{}] admitted state before vnode {vnode} failed: {error}",
            self.name
        ))
    }

    #[cfg(feature = "cluster")]
    fn reserve_remote_event_slot(
        &mut self,
        side: TemporalInputSide,
        peer: u64,
        payload_bytes: usize,
        context: &str,
    ) -> Result<(usize, usize), DbError> {
        let next_queue_bytes = self
            .queued_shuffle_bytes
            .checked_add(payload_bytes)
            .ok_or_else(|| self.accounting_error())?;
        let next_queue_events = self
            .queued_remote_events
            .checked_add(1)
            .ok_or_else(|| self.accounting_error())?;
        let current_accounted = self.checked_accounted_state_bytes()?;
        let port = side.port();
        let previous_capacity = self.peer_channels[port][&peer].events.capacity();
        let reserve_error = self.peer_channels[port]
            .get_mut(&peer)
            .expect("validated temporal peer channel")
            .events
            .try_reserve_exact(1)
            .err();
        if let Some(error) = reserve_error {
            return Err(DbError::Pipeline(format!(
                "temporal join [{}] could not reserve {context}: {error}",
                self.name
            )));
        }
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
                .expect("reserved temporal peer channel")
                .events
                .shrink_to(previous_capacity);
            let retained_capacity = self.peer_channels[port][&peer].events.capacity();
            let retained_capacity_bytes = retained_capacity
                .checked_sub(previous_capacity)
                .and_then(|slots| slots.checked_mul(REMOTE_EVENT_CHARGE))
                .ok_or_else(|| self.accounting_error())?;
            self.queued_event_capacity_bytes = self
                .queued_event_capacity_bytes
                .checked_add(retained_capacity_bytes)
                .ok_or_else(|| self.accounting_error())?;
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] {context}", self.name),
                accounted_bytes: next_accounted,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        self.queued_event_capacity_bytes = self
            .queued_event_capacity_bytes
            .checked_add(added_capacity_bytes)
            .ok_or_else(|| self.accounting_error())?;
        Ok((next_queue_bytes, next_queue_events))
    }

    #[cfg(feature = "cluster")]
    fn portable_handoff_cut(
        &self,
        transition: &ManagedVnodeTransition<'_>,
        fresh_acquirer: bool,
    ) -> Result<Option<TemporalHandoffCut>, DbError> {
        if !fresh_acquirer {
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
                    "temporal join [{}] vnode {} restore has invalid donor {}",
                    self.name, restore.vnode, restore.participant_id
                )));
            }
            donors.insert(restore.participant_id);
        }
        if donors.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] fresh owner has no acquired vnode frames",
                self.name
            )));
        }

        let predecessor_participants = transition.predecessor.participant_ids();
        let mut whole_donors = std::collections::BTreeSet::new();
        let mut common_frontiers = None;
        let mut published_watermark = None;
        let mut published_uninitialized = false;
        for restore in transition.whole_restores {
            if !whole_donors.insert(restore.participant_id)
                || restore.state.len() > self.max_managed_state_bytes
            {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] has an invalid whole frame for donor {}",
                    self.name, restore.participant_id
                )));
            }
            let preflight = self.preflight_whole_checkpoint_archive(
                restore.state,
                &format!("donor {} whole checkpoint", restore.participant_id),
                |_| Ok(()),
            )?;
            let checkpoint = with_aligned_checkpoint_bytes(restore.state, |state| {
                rkyv::from_bytes::<TemporalJoinOperatorCheckpoint, rkyv::rancor::Error>(state)
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "temporal join [{}] donor {} whole checkpoint: {error}",
                            self.name, restore.participant_id
                        ))
                    })
            })?;
            let decoded_bytes = Self::decoded_whole_checkpoint_bytes(&checkpoint)?;
            if decoded_bytes > preflight.decoded_checkpoint {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] donor {} decoded whole checkpoint exceeds its preflighted transition headroom",
                    self.name, restore.participant_id
                )));
            }
            if checkpoint.version != OPERATOR_CHECKPOINT_VERSION {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] donor {} uses unsupported checkpoint version {}",
                    self.name, restore.participant_id, checkpoint.version
                )));
            }
            let frontiers: [InputFrontier; 2] = checkpoint.frontiers.map(Into::into);
            let published_output_frontier = checkpoint.published_output_frontier.map(Into::into);
            if frontiers
                .iter()
                .any(|frontier| frontier.watermark == Some(i64::MIN))
                || published_output_frontier
                    .is_some_and(|frontier: InputFrontier| frontier.watermark == Some(i64::MIN))
            {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] donor {} has an invalid handoff frontier",
                    self.name, restore.participant_id
                )));
            }
            self.validate_published_output_frontier(frontiers, published_output_frontier)?;

            let cluster = checkpoint.cluster.ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "temporal join [{}] donor {} whole checkpoint is not from cluster mode",
                    self.name, restore.participant_id
                ))
            })?;
            if cluster.assignment_version != transition.predecessor.assignment_version
                || cluster.owner_map_digest != transition.predecessor.assignment_digest
                || cluster.self_id != restore.participant_id
            {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] donor {} whole checkpoint is outside the predecessor assignment",
                    self.name, restore.participant_id
                )));
            }
            let expected_peers = predecessor_participants
                .iter()
                .copied()
                .filter(|peer| *peer != restore.participant_id)
                .collect::<Vec<_>>();
            if cluster
                .remote_peer_cursors
                .iter()
                .flatten()
                .any(|peer| expected_peers.binary_search(peer).is_err())
            {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] donor {} has an invalid ordered-channel cursor",
                    self.name, restore.participant_id
                )));
            }
            let local_frontiers: [InputFrontier; 2] = cluster.local_frontiers.map(Into::into);
            if local_frontiers
                .iter()
                .any(|frontier| frontier.watermark == Some(i64::MIN))
            {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] donor {} has an invalid local frontier",
                    self.name, restore.participant_id
                )));
            }
            for (port, channels) in cluster.channels.into_iter().enumerate() {
                if channels.len() != expected_peers.len() {
                    return Err(DbError::Checkpoint(format!(
                        "temporal join [{}] donor {} has an incomplete peer cut",
                        self.name, restore.participant_id
                    )));
                }
                let mut applied = Vec::with_capacity(channels.len());
                for (expected_peer, channel) in expected_peers.iter().zip(channels) {
                    let frontier: InputFrontier = channel.applied.into();
                    if channel.peer != *expected_peer
                        || !channel.events.is_empty()
                        || !channel.positioned_ipc.is_empty()
                        || !channel.mutation_ipc.is_empty()
                        || frontier.watermark == Some(i64::MIN)
                    {
                        return Err(DbError::Checkpoint(format!(
                            "temporal join [{}] donor {} has retained ordered channel state",
                            self.name, restore.participant_id
                        )));
                    }
                    applied.push(frontier);
                }
                let merged = merge_input_frontier_iter(
                    std::iter::once(local_frontiers[port]).chain(applied),
                    i64::MIN,
                );
                if merged != frontiers[port] {
                    return Err(DbError::Checkpoint(format!(
                        "temporal join [{}] donor {} input frontiers do not form one common cut",
                        self.name, restore.participant_id
                    )));
                }
            }
            if common_frontiers.is_some_and(|expected| expected != frontiers) {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] donor whole checkpoints disagree on the input handoff cut",
                    self.name
                )));
            }
            common_frontiers = Some(frontiers);
            match published_output_frontier.and_then(|frontier| frontier.watermark) {
                Some(watermark) if !published_uninitialized => {
                    published_watermark = Some(
                        published_watermark
                            .map_or(watermark, |current: i64| current.min(watermark)),
                    );
                }
                _ => {
                    published_uninitialized = true;
                    published_watermark = None;
                }
            }
        }
        if whole_donors != donors {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] whole checkpoints do not exactly cover acquired vnode donors",
                self.name
            )));
        }
        let frontiers = common_frontiers.ok_or_else(|| {
            DbError::Checkpoint(format!(
                "temporal join [{}] fresh owner is missing its portable whole cut",
                self.name
            ))
        })?;
        let published_output_frontier = if published_uninitialized {
            None
        } else {
            published_watermark.map(|watermark| InputFrontier {
                watermark: Some(watermark),
                idle: false,
            })
        };
        Ok(Some(TemporalHandoffCut {
            frontiers,
            published_output_frontier,
        }))
    }

    #[cfg(feature = "cluster")]
    fn prepare_transition_image(
        &self,
        transition: &ManagedVnodeTransition<'_>,
    ) -> Result<PreparedTemporalJoinTransition, DbError> {
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "temporal join [{}] cannot transition without cluster ownership",
                self.name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
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
        if transition.target.vnode_count != self.vnode_count.get()
            || transition.target.assignment_version != assignment.version()
            || !transition.target.matches_owner_map(&owners)
            || config.sender.recovery_gen() != config.receiver.recovery_gen()
            || target_contains_self != transition.target.contains(config.self_id.0)
            || (target_contains_self && !active_transport)
            || (!target_contains_self && !inactive_transport)
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] transition target does not match active assignment {}",
                self.name,
                assignment.version()
            )));
        }
        let installed_owners = self
            .local_assignment
            .owners()
            .iter()
            .map(|owner| owner.0)
            .collect::<Vec<_>>();
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
                        "temporal join [{}] checkpoint bootstrap has an invalid predecessor owner map",
                        self.name
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
        if transition.predecessor.vnode_count != self.vnode_count.get()
            || !version_edge_valid
            || self.local_assignment.owners().len() != assignment.owners().len()
            || if checkpoint_bootstrap {
                self.local_assignment.version() != assignment.version()
                    || self.local_assignment.owners() != assignment.owners()
                    || self.vnode_states.iter().any(Option::is_some)
                    || !self.resident_vnodes.is_empty()
                    || self.frontiers != [InputFrontier::default(); 2]
                    || self.local_frontiers != [InputFrontier::default(); 2]
                    || self.published_output_frontier.is_some()
                    || self.whole_restore == WholeRestoreState::Applied
                    || self.pending_cluster_input.is_some()
                    || self.maintenance_pending
                    || self.maintenance_remaining != 0
                    || self.maintenance_rescan
            } else {
                transition.predecessor.assignment_version != self.local_assignment.version()
                    || !transition.predecessor.matches_owner_map(&installed_owners)
            }
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] transition is not adjacent to its installed assignment {}",
                self.name,
                self.local_assignment.version()
            )));
        }

        let predecessor_owned = if !checkpoint_bootstrap
            && transition
                .predecessor
                .participant_incarnation(config.self_id.0)
                == Some(config.sender.incarnation())
        {
            self.local_assignment
                .owners()
                .iter()
                .enumerate()
                .filter(|(_, owner)| **owner == config.self_id)
                .map(|(vnode, _)| {
                    u32::try_from(vnode).expect("temporal vnode topology must fit u32")
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let target_owned = assignment
            .owners()
            .iter()
            .enumerate()
            .filter(|(_, owner)| **owner == config.self_id)
            .map(|(vnode, _)| u32::try_from(vnode).expect("temporal vnode topology must fit u32"))
            .collect::<Vec<_>>();
        let fresh_acquirer =
            checkpoint_bootstrap || (predecessor_owned.is_empty() && !target_owned.is_empty());
        let expected_revoked = predecessor_owned
            .iter()
            .copied()
            .filter(|vnode| target_owned.binary_search(vnode).is_err())
            .collect::<rustc_hash::FxHashSet<_>>();
        let expected_restored = target_owned
            .iter()
            .copied()
            .filter(|vnode| predecessor_owned.binary_search(vnode).is_err())
            .collect::<Vec<_>>();
        let restored_vnodes = transition
            .restores
            .iter()
            .map(|restore| restore.vnode)
            .collect::<Vec<_>>();
        if transition.revoked != &expected_revoked
            || restored_vnodes != expected_restored
            || restored_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] transition does not match its exact ownership delta",
                self.name
            )));
        }

        if self.pending_cluster_input.is_some()
            || self.pending_frontiers.is_some()
            || self.frontier_remaining != 0
            || self.frontier_has_work
            || self.queued_shuffle_bytes != 0
            || self.queued_remote_events != 0
            || self.last_broadcasts != self.local_frontiers
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] transition requires a drained frontier and channel cut",
                self.name
            )));
        }
        let predecessor_peers = Self::remote_owner_peers(&self.local_assignment, config.self_id);
        if self.cluster_peers.as_ref() != predecessor_peers.as_slice()
            || self.peer_channels.iter().any(|channels| {
                channels.len() != predecessor_peers.len()
                    || !channels
                        .keys()
                        .copied()
                        .eq(predecessor_peers.iter().copied())
            })
            || self
                .remote_peer_cursors
                .iter()
                .flatten()
                .any(|peer| predecessor_peers.binary_search(peer).is_err())
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] transition found stale predecessor peer topology",
                self.name
            )));
        }
        let mut event_capacity_bytes = 0usize;
        for channel in self.peer_channels.iter().flat_map(BTreeMap::values) {
            if !channel.events.is_empty() || channel.accepted != channel.applied {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] transition found undrained ordered channel state",
                    self.name
                )));
            }
            event_capacity_bytes = event_capacity_bytes
                .checked_add(
                    channel
                        .events
                        .capacity()
                        .checked_mul(REMOTE_EVENT_CHARGE)
                        .ok_or_else(|| self.accounting_error())?,
                )
                .ok_or_else(|| self.accounting_error())?;
        }
        if event_capacity_bytes != self.queued_event_capacity_bytes
            || self.effective_cluster_frontiers(self.local_frontiers, None, None)? != self.frontiers
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] transition found inconsistent channel accounting or frontier",
                self.name
            )));
        }

        let current = self.derive_vnode_inventory(
            &self.local_assignment,
            config.self_id,
            self.frontiers,
            |vnode| self.vnode_states[vnode as usize].as_deref(),
        )?;
        if current.resident_vnodes != self.resident_vnodes
            || current.vnode_pending_holds != self.vnode_pending_holds
            || current.pending_hold_counts != self.pending_hold_counts
            || current.retained_state_bytes != self.retained_state_bytes
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] transition found inconsistent vnode caches",
                self.name
            )));
        }

        let live_bytes = self.checked_accounted_state_bytes()?;
        if live_bytes > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] vnode transition", self.name),
                accounted_bytes: live_bytes,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        let raw_restore_bytes = transition
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
            .ok_or_else(|| self.accounting_error())?;
        let vnode_alignment_peak = transition
            .restores
            .iter()
            .map(|restore| vnode_checkpoint_alignment_copy_charge(restore.state))
            .try_fold(0usize, |peak, charge| charge.map(|charge| peak.max(charge)))?;
        let whole_alignment_peak = transition
            .whole_restores
            .iter()
            .map(|restore| checkpoint_alignment_copy_charge(restore.state))
            .try_fold(0usize, |peak, charge| charge.map(|charge| peak.max(charge)))?;
        let transport_phase_bytes = raw_restore_bytes
            .checked_add(vnode_alignment_peak.max(whole_alignment_peak))
            .ok_or_else(|| self.accounting_error())?;
        let transport_peak = live_bytes
            .checked_add(transport_phase_bytes)
            .ok_or_else(|| self.accounting_error())?;
        if transport_peak > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] transition restore payload", self.name),
                accounted_bytes: transport_peak,
                limit_bytes: self.max_managed_state_bytes,
            });
        }

        let predecessor_participants = transition.predecessor.participant_ids();
        let mut sequential_restore_peak = vnode_alignment_peak;
        if fresh_acquirer {
            for restore in transition.whole_restores {
                let decoded_bytes = self
                    .preflight_whole_checkpoint_archive(
                    restore.state,
                    &format!("donor {} whole checkpoint", restore.participant_id),
                    |archived| {
                        let cluster = archived.cluster.as_ref().ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "temporal join [{}] donor {} whole checkpoint is not from cluster mode",
                                self.name, restore.participant_id
                            ))
                        })?;
                        if cluster.assignment_version
                            != transition.predecessor.assignment_version
                            || cluster.owner_map_digest
                                != transition.predecessor.assignment_digest
                            || cluster.self_id != restore.participant_id
                        {
                            return Err(DbError::Checkpoint(format!(
                                "temporal join [{}] donor {} whole checkpoint is outside the predecessor assignment",
                                self.name, restore.participant_id
                            )));
                        }
                        for port in 0..2 {
                            let channels = &cluster.channels[port];
                            let expected_peers = predecessor_participants
                                .iter()
                                .copied()
                                .filter(|peer| *peer != restore.participant_id);
                            if channels.len()
                                != predecessor_participants
                                    .len()
                                    .saturating_sub(1)
                                || !channels
                                    .iter()
                                    .map(|channel| channel.peer)
                                    .eq(expected_peers)
                                || channels.iter().any(|channel| {
                                    !channel.events.is_empty()
                                        || !channel.positioned_ipc.is_empty()
                                        || !channel.mutation_ipc.is_empty()
                                })
                            {
                                return Err(DbError::Checkpoint(format!(
                                    "temporal join [{}] donor {} has an invalid archived peer cut",
                                    self.name, restore.participant_id
                                )));
                            }
                        }
                        Ok(())
                    },
                )?
                    .decoded_checkpoint;
                let decode_peak = checkpoint_alignment_copy_charge(restore.state)?
                    .checked_add(decoded_bytes)
                    .ok_or_else(|| self.accounting_error())?;
                sequential_restore_peak = sequential_restore_peak.max(decode_peak);
            }
        }
        let restore_phase_bytes = raw_restore_bytes
            .checked_add(sequential_restore_peak)
            .ok_or_else(|| self.accounting_error())?;
        let restore_peak = live_bytes
            .checked_add(restore_phase_bytes)
            .ok_or_else(|| self.accounting_error())?;
        if restore_peak > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] transition restore preflight", self.name),
                accounted_bytes: restore_peak,
                limit_bytes: self.max_managed_state_bytes,
            });
        }

        let mut handoff_cut = self.portable_handoff_cut(transition, fresh_acquirer)?;
        let transition_frontiers = handoff_cut.map_or(self.frontiers, |cut| cut.frontiers);
        let mut published_output_frontier = handoff_cut
            .map_or(self.published_output_frontier, |cut| {
                cut.published_output_frontier
            });

        let mut replacements = BTreeMap::new();
        for &vnode in transition.revoked {
            replacements.insert(vnode, None);
        }
        let mut restored_state_bytes = 0usize;
        for restore in transition.restores {
            let limit = if restore.state.first() == Some(&PRESENT_VNODE) {
                self.max_managed_state_bytes
                    .checked_sub(live_bytes)
                    .and_then(|bytes| bytes.checked_sub(restore_phase_bytes))
                    .and_then(|bytes| bytes.checked_sub(restored_state_bytes))
                    .and_then(|bytes| bytes.checked_sub(PENDING_HOLD_ENTRY_CHARGE))
                    .filter(|bytes| *bytes != 0)
                    .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                        context: format!(
                            "temporal join [{}] vnode {} transition restore",
                            self.name, restore.vnode
                        ),
                        accounted_bytes: live_bytes.saturating_add(restored_state_bytes),
                        limit_bytes: self.max_managed_state_bytes,
                    })?
            } else {
                1
            };
            let state = self.decode_vnode_frame(
                restore.vnode,
                transition.target.vnode_count,
                restore.state,
                limit,
            )?;
            if let Some(state) = state.as_ref() {
                if Self::vnode_state_frontiers(state) != transition_frontiers {
                    return Err(DbError::Checkpoint(format!(
                        "temporal join [{}] restored vnode {} is outside the transition frontier cut",
                        self.name, restore.vnode
                    )));
                }
                restored_state_bytes = restored_state_bytes
                    .checked_add(state.accounted_state_bytes())
                    .ok_or_else(|| self.accounting_error())?;
            }
            replacements.insert(restore.vnode, state);
        }

        let target_inventory = self.derive_vnode_inventory(
            &assignment,
            config.self_id,
            transition_frontiers,
            |vnode| match replacements.get(&vnode) {
                Some(state) => state.as_deref(),
                None => self.vnode_states[vnode as usize].as_deref(),
            },
        )?;
        let target_hold = target_inventory
            .pending_hold_counts
            .first_key_value()
            .map(|(hold, _)| *hold);
        if let (Some(published), Some(hold)) = (
            published_output_frontier.and_then(|frontier| frontier.watermark),
            target_hold,
        ) {
            if hold < published {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] restored hold {hold} precedes published output frontier {published}",
                    self.name
                )));
            }
        }
        if let Some(frontier) = published_output_frontier.as_mut() {
            *frontier = frontier.held_at(target_hold);
        }
        if let Some(cut) = handoff_cut.as_mut() {
            cut.published_output_frontier = published_output_frontier;
        }

        let target_peers = Self::remote_owner_peers(&assignment, config.self_id);
        let mut peer_channels = [BTreeMap::new(), BTreeMap::new()];
        for port in 0..2 {
            for &peer in &target_peers {
                let channel = if fresh_acquirer {
                    TemporalPeerChannel {
                        applied: transition_frontiers[port],
                        accepted: transition_frontiers[port],
                        events: VecDeque::new(),
                    }
                } else {
                    self.peer_channels[port].get(&peer).map_or(
                        TemporalPeerChannel {
                            applied: transition_frontiers[port],
                            accepted: transition_frontiers[port],
                            events: VecDeque::new(),
                        },
                        |channel| TemporalPeerChannel {
                            applied: channel.applied,
                            accepted: channel.accepted,
                            events: VecDeque::new(),
                        },
                    )
                };
                peer_channels[port].insert(peer, channel);
            }
            let local_frontier = if fresh_acquirer {
                transition_frontiers[port]
            } else {
                self.local_frontiers[port]
            };
            let merged = merge_input_frontier_iter(
                std::iter::once(local_frontier)
                    .chain(peer_channels[port].values().map(|channel| channel.applied)),
                i64::MIN,
            );
            validate_frontier(
                transition_frontiers[port],
                merged,
                if port == 0 { "left" } else { "right" },
                &self.name,
            )?;
        }

        let bootstrap_broadcast = !target_owned.is_empty() && !target_peers.is_empty();
        let prepared = PreparedTemporalJoinTransition {
            slots: replacements.into_iter().collect(),
            local_assignment: assignment,
            resident_vnodes: target_inventory.resident_vnodes,
            vnode_pending_holds: target_inventory.vnode_pending_holds,
            pending_hold_counts: target_inventory.pending_hold_counts,
            retained_state_bytes: target_inventory.retained_state_bytes,
            maintenance_pending: target_inventory.maintenance_pending,
            cluster_peers: target_peers.into(),
            peer_channels,
            bootstrap_broadcast,
            handoff_cut,
        };
        let total = live_bytes
            .checked_add(restore_phase_bytes)
            .and_then(|bytes| bytes.checked_add(Self::transition_accounted_bytes(&prepared)))
            .ok_or_else(|| self.accounting_error())?;
        if total > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] prepared vnode transition", self.name),
                accounted_bytes: total,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        Ok(prepared)
    }

    fn validate_vnode_roster(
        &self,
        required_vnodes: &[u32],
        vnode_count: u32,
    ) -> Result<(), DbError> {
        if vnode_count != u32::from(self.key_group_count)
            || required_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
            || required_vnodes.iter().any(|vnode| *vnode >= vnode_count)
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] received a non-canonical vnode roster {required_vnodes:?} for vnode_count {vnode_count}",
                self.name
            )));
        }
        if let Some(unowned) = self
            .resident_vnodes
            .iter()
            .copied()
            .find(|vnode| required_vnodes.binary_search(vnode).is_err())
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] retained unowned vnode state {unowned}",
                self.name
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl GraphOperator for ManagedTemporalJoinOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::managed_temporal_join()
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        #[cfg(feature = "cluster")]
        let (prepared, retired) = {
            let prepared = self
                .prepared_vnode_transition
                .as_ref()
                .map_or(0, Self::transition_accounted_bytes);
            match self.vnode_transition_cleanup.as_ref() {
                Some(TemporalJoinTransitionCleanup::Aborted(transition)) => (
                    prepared.saturating_add(Self::transition_accounted_bytes(transition)),
                    0,
                ),
                Some(TemporalJoinTransitionCleanup::Published(transition)) => {
                    (prepared, Self::transition_accounted_bytes(transition))
                }
                None => (prepared, 0),
            }
        };
        #[cfg(not(feature = "cluster"))]
        let (prepared, retired) = (0, 0);
        Some(ManagedStateAccountingSnapshot {
            live: self.checked_accounted_state_bytes().unwrap_or(usize::MAX),
            prepared,
            retired,
        })
    }

    fn set_managed_state_budget(&mut self, bytes: usize) {
        self.max_managed_state_bytes = bytes;
    }

    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        self.projection
            .initialize(&self.projection_input_schema)
            .await?;
        let accounted = self.checked_accounted_state_bytes()?;
        if accounted > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] topology", self.name),
                accounted_bytes: accounted,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        Ok(())
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let frontier = |index: usize| InputFrontier {
            watermark: watermarks
                .get(index)
                .copied()
                .filter(|watermark| *watermark != i64::MIN),
            idle: false,
        };
        #[cfg(feature = "cluster")]
        if self.cluster_shuffle.is_some() {
            return self
                .process_cluster(inputs, [frontier(0), frontier(1)])
                .await;
        }
        self.process_and_project(inputs, [frontier(0), frontier(1)])
            .await
    }

    async fn process_with_frontiers(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: &[InputFrontier],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if frontiers.len() != 2 {
            return Err(DbError::InvalidOperation(format!(
                "temporal join [{}] requires two input frontiers",
                self.name
            )));
        }
        #[cfg(feature = "cluster")]
        if self.cluster_shuffle.is_some() {
            return self
                .process_cluster(inputs, [frontiers[0], frontiers[1]])
                .await;
        }
        self.process_and_project(inputs, [frontiers[0], frontiers[1]])
            .await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let Some(capture) = self.capture_operator_checkpoint(usize::MAX)? else {
            return Ok(None);
        };
        let data = capture.encode(self.max_managed_state_bytes)?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn checkpoint_capture(
        &mut self,
        max_capture_bytes: u64,
    ) -> Result<Option<StateFrameCapture>, DbError> {
        let Some(capture) = self.capture_operator_checkpoint(
            usize::try_from(max_capture_bytes).unwrap_or(usize::MAX),
        )?
        else {
            return Ok(None);
        };
        let retained_bytes = u64::try_from(capture.retained_bytes()).unwrap_or(u64::MAX);
        let max_managed_state_bytes = self.max_managed_state_bytes;
        Ok(Some(StateFrameCapture::deferred(
            retained_bytes,
            move |remaining| {
                let data = capture.encode(remaining.min(max_managed_state_bytes))?;
                Ok(EncodedStateFrame::from_vec(data))
            },
        )))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        let cluster_pristine = self.local_frontiers == [InputFrontier::default(); 2]
            && self.pending_cluster_input.is_none()
            && self.last_broadcasts == [InputFrontier::default(); 2]
            && self.remote_peer_cursors == [None; 2]
            && self.queued_shuffle_bytes == 0
            && self.queued_remote_events == 0
            && self.queued_event_capacity_bytes == 0
            && self.peer_channels.iter().flatten().all(|(_, channel)| {
                channel.applied == InputFrontier::default()
                    && channel.accepted == InputFrontier::default()
                    && channel.events.is_empty()
            });
        #[cfg(not(feature = "cluster"))]
        let cluster_pristine = true;
        if !cluster_pristine
            || self.whole_restore == WholeRestoreState::Applied
            || !self.resident_vnodes.is_empty()
            || self.frontiers != [InputFrontier::default(); 2]
            || self.pending_frontiers.is_some()
            || self.published_output_frontier.is_some()
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] operator checkpoint was restored more than once",
                self.name
            )));
        }
        let OperatorCheckpoint { data } = checkpoint;
        let live_bytes = self.checked_accounted_state_bytes()?;
        let encoded_frame_bytes = checkpoint_allocation_bytes(data.capacity())?
            .checked_add(checkpoint_alignment_copy_charge(&data)?)
            .ok_or_else(|| self.accounting_error())?;
        let encoded_peak = live_bytes
            .checked_add(encoded_frame_bytes)
            .ok_or_else(|| self.accounting_error())?;
        if encoded_peak > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "temporal join [{}] operator checkpoint restore payload",
                    self.name
                ),
                accounted_bytes: encoded_peak,
                limit_bytes: self.max_managed_state_bytes,
            });
        }

        let restore_preflight = self.preflight_whole_checkpoint_archive(
            &data,
            "operator checkpoint",
            |archived| {
                #[cfg(feature = "cluster")]
                match (self.cluster_shuffle.as_ref(), archived.cluster.as_ref()) {
                    (Some(config), Some(cluster)) => {
                        let (_, assignment, peers) = self.active_cluster_scope()?;
                        if cluster.assignment_version != assignment.version()
                            || cluster.owner_map_digest != self.owner_map_digest(&assignment)
                            || cluster.self_id != config.self_id.0
                        {
                            return Err(DbError::Checkpoint(format!(
                                "temporal join [{}] archived cluster checkpoint identity is invalid",
                                self.name
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
                                    "temporal join [{}] archived cluster channel roster is invalid",
                                    self.name
                                )));
                            }
                        }
                    }
                    (None, None) => {}
                    _ => {
                        return Err(DbError::Checkpoint(format!(
                            "temporal join [{}] archived checkpoint deployment mode does not match the operator",
                            self.name
                        )));
                    }
                }
                #[cfg(not(feature = "cluster"))]
                if archived.cluster.is_some() {
                    return Err(DbError::Checkpoint(format!(
                        "temporal join [{}] archived checkpoint contains cluster channel state",
                        self.name
                    )));
                }
                Ok(())
            },
        )?;
        let preflight_peak = encoded_peak
            .checked_add(restore_preflight.decoded_checkpoint)
            .ok_or_else(|| self.accounting_error())?;
        if preflight_peak > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "temporal join [{}] operator checkpoint restore preflight",
                    self.name
                ),
                accounted_bytes: preflight_peak,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        let runtime_peak = live_bytes
            .checked_add(restore_preflight.decoded_checkpoint)
            .and_then(|bytes| bytes.checked_add(restore_preflight.runtime_scratch))
            .ok_or_else(|| self.accounting_error())?;
        if runtime_peak > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "temporal join [{}] cluster checkpoint restore preflight",
                    self.name
                ),
                accounted_bytes: runtime_peak,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        let checkpoint = with_aligned_checkpoint_bytes(&data, |data| {
            rkyv::from_bytes::<TemporalJoinOperatorCheckpoint, rkyv::rancor::Error>(data).map_err(
                |error| {
                    DbError::Checkpoint(format!(
                        "temporal join [{}] operator checkpoint: {error}",
                        self.name
                    ))
                },
            )
        })?;
        let decoded_checkpoint_bytes = Self::decoded_whole_checkpoint_bytes(&checkpoint)?;
        if decoded_checkpoint_bytes > restore_preflight.decoded_checkpoint {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] decoded operator checkpoint exceeds its preflighted bound",
                self.name
            )));
        }
        drop(data);
        if checkpoint.version != OPERATOR_CHECKPOINT_VERSION {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] unsupported operator checkpoint version {}",
                self.name, checkpoint.version
            )));
        }
        let TemporalJoinOperatorCheckpoint {
            version: _,
            frontiers,
            maintenance_cursor,
            maintenance_pending,
            maintenance_remaining,
            maintenance_rescan,
            published_output_frontier,
            cluster,
        } = checkpoint;
        let frontiers: [InputFrontier; 2] = frontiers.map(Into::into);
        if frontiers
            .iter()
            .any(|frontier: &InputFrontier| frontier.watermark == Some(i64::MIN))
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] operator checkpoint uses the uninitialized frontier sentinel",
                self.name
            )));
        }
        let published_output_frontier: Option<InputFrontier> =
            published_output_frontier.map(Into::into);
        if published_output_frontier
            .is_some_and(|frontier: InputFrontier| frontier.watermark == Some(i64::MIN))
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] operator checkpoint has an invalid output frontier",
                self.name
            )));
        }
        self.validate_published_output_frontier(frontiers, published_output_frontier)?;
        let cursor = usize::try_from(maintenance_cursor).map_err(|_| {
            DbError::Checkpoint(format!(
                "temporal join [{}] maintenance cursor exceeds usize",
                self.name
            ))
        })?;
        if cursor >= self.vnode_states.len() {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] maintenance cursor {cursor} is outside its vnode table",
                self.name
            )));
        }
        let remaining = usize::try_from(maintenance_remaining).map_err(|_| {
            DbError::Checkpoint(format!(
                "temporal join [{}] maintenance sweep exceeds usize",
                self.name
            ))
        })?;
        if remaining > self.vnode_states.len()
            || (!maintenance_pending && (remaining != 0 || maintenance_rescan))
            || (maintenance_pending && remaining == 0 && !maintenance_rescan)
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] operator checkpoint has invalid maintenance state",
                self.name
            )));
        }

        #[cfg(feature = "cluster")]
        let decoded_cluster = match (self.cluster_shuffle.is_some(), cluster) {
            (true, Some(cluster)) => Some(self.decode_cluster_checkpoint(
                cluster,
                frontiers,
                decoded_checkpoint_bytes,
            )?),
            (false, None) => None,
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] checkpoint deployment mode does not match the restored operator",
                    self.name
                )));
            }
        };
        #[cfg(not(feature = "cluster"))]
        if cluster.is_some() {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] checkpoint contains cluster channel state",
                self.name
            )));
        }

        #[cfg(feature = "cluster")]
        if let Some(cluster) = decoded_cluster.as_ref() {
            let restored_cluster_bytes = self.decoded_cluster_runtime_bytes(cluster)?;
            if restored_cluster_bytes > restore_preflight.runtime_scratch {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] restored cluster state exceeds its preflighted bound",
                    self.name
                )));
            }
            let restored_peak = live_bytes
                .checked_add(decoded_checkpoint_bytes)
                .and_then(|bytes| bytes.checked_add(restored_cluster_bytes))
                .ok_or_else(|| self.accounting_error())?;
            if restored_peak > self.max_managed_state_bytes {
                return Err(DbError::ManagedStateBudgetExceeded {
                    context: format!(
                        "temporal join [{}] cluster checkpoint restore peak",
                        self.name
                    ),
                    accounted_bytes: restored_peak,
                    limit_bytes: self.max_managed_state_bytes,
                });
            }
        }

        self.frontiers = frontiers;
        self.maintenance_cursor = cursor;
        self.maintenance_pending = maintenance_pending;
        self.maintenance_remaining = remaining;
        self.maintenance_rescan = maintenance_rescan;
        self.published_output_frontier = published_output_frontier;
        #[cfg(feature = "cluster")]
        if let Some(cluster) = decoded_cluster {
            self.local_frontiers = cluster.local_frontiers;
            self.last_broadcasts = cluster.local_frontiers;
            self.peer_channels = cluster.peer_channels;
            self.remote_peer_cursors = cluster.remote_peer_cursors;
            self.queued_shuffle_bytes = cluster.queued_shuffle_bytes;
            self.queued_remote_events = cluster.queued_remote_events;
            self.queued_event_capacity_bytes = cluster.queued_event_capacity_bytes;
        }
        self.whole_restore = WholeRestoreState::Applied;
        Ok(())
    }

    fn wants_input(&self) -> bool {
        let ready = self.pending_frontiers.is_none() && !self.maintenance_pending;
        #[cfg(feature = "cluster")]
        let ready = ready
            && self.pending_cluster_input.is_none()
            && !self.has_remote_events()
            && self.last_broadcasts == self.local_frontiers;
        ready
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_aligned_replay_pending(&self) -> bool {
        self.pending_cluster_input.is_some() || self.has_remote_events()
    }

    fn checkpoint_drain_pending(&self) -> bool {
        let pending = self.pending_frontiers.is_some();
        #[cfg(feature = "cluster")]
        let pending = pending
            || self.pending_cluster_input.is_some()
            || self.last_broadcasts != self.local_frontiers;
        pending
    }

    #[cfg(feature = "cluster")]
    fn deferred_work_is_runnable(&self) -> bool {
        self.pending_frontiers.is_some()
            || self.maintenance_pending
            || self.has_remote_events()
            || (self.pending_cluster_input.is_none()
                && self.last_broadcasts != self.local_frontiers)
    }

    fn advances_frontier_without_input(&self) -> bool {
        true
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
        let (peer, batch) = self.prepare_remote_batch(side, batch, &config, &assignment, &peers)?;
        let (next_queue_bytes, next_queue_events) = self.reserve_remote_event_slot(
            side,
            peer,
            batch.charged_bytes,
            "ordered shuffle queue",
        )?;
        self.queued_shuffle_bytes = next_queue_bytes;
        self.queued_remote_events = next_queue_events;
        self.peer_channels[side.port()]
            .get_mut(&peer)
            .expect("reserved temporal peer channel")
            .events
            .push_back(TemporalRemoteEvent {
                assignment_version: batch
                    .retained
                    .assignment_version()
                    .expect("validated temporal assignment"),
                recovery_gen: batch
                    .retained
                    .recovery_gen()
                    .expect("validated temporal recovery"),
                payload: TemporalRemoteEventPayload::Data(batch),
            });
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle_frontier(
        &mut self,
        stage: &str,
        peer: u64,
        frontier: InputFrontier,
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
                "temporal join [{}] received {} frontier from peer {peer} outside assignment {} recovery {}",
                self.name,
                side.name(),
                assignment.version(),
                config.receiver.recovery_gen()
            )));
        }
        let previous = self.peer_channels[side.port()][&peer].accepted;
        if previous.watermark.is_some() && frontier.watermark.is_none() {
            validate_frontier(previous, frontier, side.name(), &self.name)?;
        }
        let frontier = self.normalize_remote_frontier(side, frontier);
        validate_frontier(previous, frontier, side.name(), &self.name)?;
        let (next_queue_bytes, next_queue_events) =
            self.reserve_remote_event_slot(side, peer, 0, "ordered frontier queue")?;
        self.queued_shuffle_bytes = next_queue_bytes;
        self.queued_remote_events = next_queue_events;
        let channel = self.peer_channels[side.port()]
            .get_mut(&peer)
            .expect("reserved temporal peer channel");
        channel.events.push_back(TemporalRemoteEvent {
            assignment_version,
            recovery_gen,
            payload: TemporalRemoteEventPayload::Frontier(frontier),
        });
        channel.accepted = frontier;
        Ok(())
    }

    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        let mut output = self
            .published_output_frontier
            .unwrap_or_else(|| self.derive_output_frontier(input));
        if self.maintenance_pending {
            output.idle = false;
        }
        #[cfg(feature = "cluster")]
        if self.pending_cluster_input.is_some() || self.has_remote_events() {
            output.idle = false;
        }
        output
    }

    #[cfg(feature = "cluster")]
    fn restored_output_frontier(&self) -> Option<InputFrontier> {
        self.published_output_frontier.map(|mut frontier| {
            if self.maintenance_pending
                || self.pending_cluster_input.is_some()
                || self.has_remote_events()
            {
                frontier.idle = false;
            }
            frontier
        })
    }

    fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
        max_capture_bytes: u64,
    ) -> Result<Option<Vec<CapturedVnodeState>>, DbError> {
        #[cfg(feature = "cluster")]
        if self.pending_cluster_input.is_some() {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] cannot capture vnodes while a shuffle send is pending",
                self.name
            )));
        }
        if self.pending_frontiers.is_some() {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] cannot capture vnodes during bounded frontier fanout",
                self.name
            )));
        }
        self.validate_vnode_roster(required_vnodes, vnode_count)?;
        let full_capture = self.vnode_capture_mode == VnodeCaptureMode::Full;
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
            self.vnode_capture_mode = VnodeCaptureMode::Sparse;
            self.clear_dirty_vnodes();
            return Ok(Some(Vec::new()));
        }

        let absent_frame_bytes = capture_vnodes
            .iter()
            .filter(|vnode| self.vnode_states[**vnode as usize].is_none())
            .count();
        let remaining_operator_bytes = self
            .max_managed_state_bytes
            .checked_sub(absent_frame_bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "temporal join [{}] vnode checkpoint limit exhausted",
                    self.name
                ))
            })?;
        let remaining_operator_bytes = Arc::new(AtomicUsize::new(remaining_operator_bytes));
        let mut result = Vec::with_capacity(capture_vnodes.len());
        let mut remaining_capture_bytes = max_capture_bytes;
        for vnode in capture_vnodes.iter().copied() {
            let state = if let Some(state) = self.vnode_states[vnode as usize].as_ref() {
                let capture = state.capture_checkpoint(
                    usize::try_from(remaining_capture_bytes).unwrap_or(usize::MAX),
                )?;
                let retained_bytes = u64::try_from(capture.retained_bytes()).unwrap_or(u64::MAX);
                let max_managed_state_bytes = self.max_managed_state_bytes;
                let remaining_operator_bytes = Arc::clone(&remaining_operator_bytes);
                StateFrameCapture::deferred(retained_bytes, move |remaining| {
                    let operator_remaining = remaining_operator_bytes.load(AtomicOrdering::Relaxed);
                    let frame_limit = remaining
                        .min(operator_remaining)
                        .min(max_managed_state_bytes);
                    let frame = capture.encode(frame_limit, Some(PRESENT_VNODE))?;
                    remaining_operator_bytes
                        .fetch_update(
                            AtomicOrdering::Relaxed,
                            AtomicOrdering::Relaxed,
                            |remaining| remaining.checked_sub(frame.len()),
                        )
                        .map_err(|_| {
                            DbError::Checkpoint(format!(
                                "temporal join vnode {vnode} checkpoints exceeded their managed-state limit"
                            ))
                        })?;
                    Ok(EncodedStateFrame::from_vec(frame))
                })
            } else {
                StateFrameCapture::encoded_static(&[ABSENT_VNODE])
            };
            remaining_capture_bytes = remaining_capture_bytes
                .checked_sub(state.retained_bytes())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "temporal join [{}] vnode {vnode} capture exceeded the remaining capture budget",
                        self.name
                    ))
                })?;
            result.push(CapturedVnodeState {
                vnode,
                state: Some(state),
            });
        }
        self.vnode_capture_mode = VnodeCaptureMode::Sparse;
        self.clear_dirty_vnodes();
        Ok(Some(result))
    }

    fn restore_vnode(&mut self, vnode: u32, vnode_count: u32, bytes: &[u8]) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        if self.pending_cluster_input.is_some() {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] cannot restore vnode state while a shuffle send is pending",
                self.name
            )));
        }
        let current_bytes = self
            .vnode_states
            .get(vnode as usize)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "temporal join [{}] vnode {vnode} restore does not match its {vnode_count}-vnode topology",
                    self.name
                ))
            })?
            .as_ref()
            .map_or(0, |state| state.accounted_state_bytes());
        let total = self.checked_accounted_state_bytes()?;
        let other_bytes = total
            .checked_sub(current_bytes)
            .ok_or_else(|| self.accounting_error())?;
        let restore_frame_bytes = bytes
            .len()
            .checked_add(vnode_checkpoint_alignment_copy_charge(bytes)?)
            .ok_or_else(|| self.accounting_error())?;
        let transport_peak = other_bytes
            .checked_add(restore_frame_bytes)
            .ok_or_else(|| self.accounting_error())?;
        if transport_peak > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "temporal join [{}] vnode {vnode} restore payload",
                    self.name
                ),
                accounted_bytes: transport_peak,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        let limit = if bytes.first() == Some(&PRESENT_VNODE) {
            self.max_managed_state_bytes
                .checked_sub(other_bytes)
                .and_then(|limit| limit.checked_sub(restore_frame_bytes))
                .and_then(|limit| limit.checked_sub(PENDING_HOLD_ENTRY_CHARGE))
                .filter(|limit| *limit != 0)
                .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                    context: format!("temporal join [{}] vnode {vnode} restore", self.name),
                    accounted_bytes: transport_peak.saturating_add(PENDING_HOLD_ENTRY_CHARGE),
                    limit_bytes: self.max_managed_state_bytes,
                })?
        } else {
            1
        };
        let replacement = self.decode_vnode_frame(vnode, vnode_count, bytes, limit)?;
        if let Some(state) = replacement.as_ref() {
            let restored = Self::vnode_state_frontiers(state);
            if self.whole_restore == WholeRestoreState::Applied
                && !self.maintenance_pending
                && (state.has_ready_probes() || state.has_history_gc_work())
            {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] whole checkpoint omits restored maintenance work",
                    self.name
                )));
            }
            if self.whole_restore == WholeRestoreState::Applied && self.frontiers != restored {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] whole and vnode frontiers disagree",
                    self.name
                )));
            }
            if self
                .restored_frontiers
                .is_some_and(|expected| expected != restored)
            {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] restored vnode frontiers disagree",
                    self.name
                )));
            }
            self.restored_frontiers = Some(restored);
            self.frontiers = restored;
        }
        let replacement_bytes = replacement
            .as_ref()
            .map_or(0, |state| state.accounted_state_bytes());
        self.retained_state_bytes = self
            .retained_state_bytes
            .checked_sub(current_bytes)
            .and_then(|bytes| bytes.checked_add(replacement_bytes))
            .ok_or_else(|| self.accounting_error())?;
        let present = replacement.is_some();
        self.vnode_states[vnode as usize] = replacement;
        self.mark_vnode_dirty(vnode);
        self.vnode_capture_mode = VnodeCaptureMode::Full;
        if present {
            self.add_resident_vnode(vnode);
        } else {
            self.remove_resident_vnode(vnode);
        }
        self.refresh_vnode_pending_hold(vnode)?;
        let accounted = self.checked_accounted_state_bytes()?;
        if accounted > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] vnode {vnode} restore", self.name),
                accounted_bytes: accounted,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        if self.whole_restore == WholeRestoreState::Pending {
            let restored_has_work = self.vnode_states[vnode as usize]
                .as_ref()
                .is_some_and(|state| state.has_ready_probes() || state.has_history_gc_work());
            self.maintenance_pending |= restored_has_work;
            if self.maintenance_pending {
                self.maintenance_remaining = self.resident_vnodes.len();
                self.maintenance_rescan = false;
            }
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn prepare_vnode_transition(
        &mut self,
        transition: ManagedVnodeTransition<'_>,
    ) -> Result<(), DbError> {
        if self.prepared_vnode_transition.is_some() || self.vnode_transition_cleanup.is_some() {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] already owns vnode transition state",
                self.name
            )));
        }
        self.prepared_vnode_transition = Some(self.prepare_transition_image(&transition)?);
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn abort_vnode_transition(&mut self) {
        let Some(prepared) = self.prepared_vnode_transition.take() else {
            return;
        };
        assert!(self.vnode_transition_cleanup.is_none());
        self.vnode_transition_cleanup = Some(TemporalJoinTransitionCleanup::Aborted(prepared));
    }

    #[cfg(feature = "cluster")]
    fn publish_vnode_transition(&mut self) {
        let mut prepared = self
            .prepared_vnode_transition
            .take()
            .expect("temporal join transition must be prepared before publication");
        assert!(self.vnode_transition_cleanup.is_none());
        for (vnode, state) in &mut prepared.slots {
            let index = *vnode as usize;
            std::mem::swap(&mut self.vnode_states[index], state);
            self.mark_vnode_dirty(*vnode);
        }
        self.vnode_capture_mode = VnodeCaptureMode::Full;
        std::mem::swap(&mut self.local_assignment, &mut prepared.local_assignment);
        std::mem::swap(&mut self.resident_vnodes, &mut prepared.resident_vnodes);
        std::mem::swap(
            &mut self.vnode_pending_holds,
            &mut prepared.vnode_pending_holds,
        );
        std::mem::swap(
            &mut self.pending_hold_counts,
            &mut prepared.pending_hold_counts,
        );
        std::mem::swap(
            &mut self.retained_state_bytes,
            &mut prepared.retained_state_bytes,
        );
        if let Some(cut) = prepared.handoff_cut {
            self.frontiers = cut.frontiers;
            self.local_frontiers = cut.frontiers;
            self.published_output_frontier = cut.published_output_frontier;
        }
        self.maintenance_cursor = 0;
        self.maintenance_pending = prepared.maintenance_pending;
        self.maintenance_remaining = if self.maintenance_pending {
            self.resident_vnodes.len()
        } else {
            0
        };
        self.maintenance_rescan = false;
        std::mem::swap(&mut self.cluster_peers, &mut prepared.cluster_peers);
        std::mem::swap(&mut self.peer_channels, &mut prepared.peer_channels);
        self.last_broadcasts = if prepared.bootstrap_broadcast {
            [InputFrontier::default(); 2]
        } else {
            self.local_frontiers
        };
        self.remote_peer_cursors = [None; 2];
        self.queued_shuffle_bytes = 0;
        self.queued_remote_events = 0;
        self.queued_event_capacity_bytes = 0;
        self.vnode_transition_cleanup = Some(TemporalJoinTransitionCleanup::Published(prepared));
    }

    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {
        self.vnode_transition_cleanup = None;
    }

    fn force_full_vnode_capture(&mut self) {
        self.vnode_capture_mode = VnodeCaptureMode::Full;
    }
}

fn column_index(
    schema: &SchemaRef,
    column: &str,
    operator: &str,
    role: &str,
) -> Result<usize, DbError> {
    schema.index_of(column).map_err(|error| {
        DbError::Config(format!(
            "temporal join [{operator}] {role} column '{column}': {error}"
        ))
    })
}

#[cfg(feature = "cluster")]
fn max_watermark(current: Option<i64>, floor: Option<i64>) -> Option<i64> {
    match (current, floor) {
        (Some(current), Some(floor)) => Some(current.max(floor)),
        (None, floor) => floor,
        (current, None) => current,
    }
}

fn validate_frontier(
    previous: InputFrontier,
    next: InputFrontier,
    side: &str,
    operator: &str,
) -> Result<(), DbError> {
    if previous.watermark.is_some() && next.watermark.is_none() {
        return Err(DbError::Pipeline(format!(
            "temporal join [{operator}] {side} frontier became uninitialized"
        )));
    }
    if let (Some(previous), Some(next)) = (previous.watermark, next.watermark) {
        if next < previous {
            return Err(DbError::Pipeline(format!(
                "temporal join [{operator}] {side} frontier regressed from {previous} to {next}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BinaryArray, Int64Array, StringArray, TimestampMillisecondArray, UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use laminar_connectors::connector::{
        schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
        SourceBatch, SourceMutation, SourceRowPositionCapability, SourceRowPositions,
        SOURCE_ORDER_KEY_COLUMN, SOURCE_PARTITION_COLUMN, SOURCE_SUB_OFFSET_COLUMN,
    };
    use laminar_sql::temporal::{TemporalJoinKind, TemporalProbeSchedule};

    fn visible_schemas() -> (SchemaRef, SchemaRef) {
        let left = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("venue", DataType::Utf8, false),
            Field::new(
                "trade_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("trade_id", DataType::Int64, false),
        ]));
        let right = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("venue", DataType::Utf8, false),
            Field::new(
                "quote_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Utf8, false),
        ]));
        (left, right)
    }

    fn limits(ready_probe_budget: usize) -> TemporalJoinExecutionLimits {
        TemporalJoinExecutionLimits {
            left_allowed_lateness_ms: 0,
            right_allowed_lateness_ms: 0,
            history_retention_ms: 10_000,
            max_pending_probes: 100,
            ready_probe_budget: NonZeroUsize::new(ready_probe_budget).unwrap(),
            history_gc_budget: NonZeroUsize::new(8).unwrap(),
            maintenance_vnode_budget: NonZeroUsize::new(1).unwrap(),
        }
    }

    fn config() -> TemporalJoinTranslatorConfig {
        TemporalJoinTranslatorConfig {
            left_table: "trades".into(),
            right_table: "quotes".into(),
            left_key_columns: vec!["symbol".into(), "venue".into()],
            right_key_columns: vec!["symbol".into(), "venue".into()],
            left_time_column: "trade_time".into(),
            right_time_column: "quote_time".into(),
            join_kind: TemporalJoinKind::Left,
            probe_schedule: TemporalProbeSchedule::as_of(),
            probe_alias: None,
        }
    }

    fn operator(ready_probe_budget: usize) -> (ManagedTemporalJoinOperator, SchemaRef, SchemaRef) {
        operator_with_projection(ready_probe_budget, None)
    }

    fn operator_with_projection(
        ready_probe_budget: usize,
        projection_sql: Option<&str>,
    ) -> (ManagedTemporalJoinOperator, SchemaRef, SchemaRef) {
        let (left_visible, right_visible) = visible_schemas();
        let left = schema_with_source_row_positions(&left_visible).unwrap();
        let right = schema_with_source_row_positions(&right_visible).unwrap();
        let operator = ManagedTemporalJoinOperator::try_new(
            "temporal",
            config(),
            projection_sql.map(Arc::from),
            SessionContext::new(),
            Arc::clone(&left),
            Arc::clone(&right),
            KeyGroupCount::try_from(2_u16).unwrap(),
            limits(ready_probe_budget),
        )
        .unwrap();
        (operator, left, right)
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn terminal_errors_survive_every_temporal_post_admission_wrapper() {
        fn assert_terminal(error: DbError, expected: &str) {
            let DbError::ShuffleTerminal(reason) = error else {
                panic!("expected permanent shuffle halt, got {error}");
            };
            assert_eq!(reason, expected);
        }

        let (operator, _, _) = operator(1);
        assert_terminal(
            operator.outbound_finalize_error(DbError::ShuffleTerminal("outbound".into())),
            "outbound",
        );
        assert_terminal(
            operator.remote_replay_error(DbError::ShuffleTerminal("remote replay".into())),
            "remote replay",
        );
        assert_terminal(
            operator.post_projection_error(DbError::ShuffleTerminal("post projection".into())),
            "post projection",
        );
        assert_terminal(
            operator.after_apply_error(true, 1, DbError::ShuffleTerminal("after apply".into())),
            "after apply",
        );
    }

    fn positions(rows: usize, first: u64) -> SourceRowPositions {
        let partitions = std::iter::repeat_n(b"p0".as_slice(), rows);
        let orders: Vec<[u8; 8]> = (first..first + rows as u64).map(u64::to_be_bytes).collect();
        SourceRowPositions::try_new(
            BinaryArray::from_iter_values(partitions),
            BinaryArray::from_iter_values(orders.iter()),
            UInt32Array::from(vec![0; rows]),
        )
        .unwrap()
    }

    fn left_batch(keys: &[String], venues: &[&str], times: &[i64], ids: &[i64]) -> RecordBatch {
        let (visible, _) = visible_schemas();
        let rows = RecordBatch::try_new(
            Arc::clone(&visible),
            vec![
                Arc::new(StringArray::from_iter_values(keys)),
                Arc::new(StringArray::from(venues.to_vec())),
                Arc::new(TimestampMillisecondArray::from(times.to_vec())),
                Arc::new(Int64Array::from(ids.to_vec())),
            ],
        )
        .unwrap();
        let positioned = schema_with_source_row_positions(&visible).unwrap();
        let mutations = schema_with_source_mutations_and_row_positions(&visible).unwrap();
        SourceBatch::positioned(rows, positions(keys.len(), 100))
            .unwrap()
            .into_records_with_metadata(
                SourceRowPositionCapability::OrderedDeterministic,
                &positioned,
                &mutations,
            )
            .unwrap()
    }

    fn right_batch(
        keys: &[String],
        venues: &[&str],
        times: &[i64],
        values: &[&str],
        mutations: &[SourceMutation],
    ) -> RecordBatch {
        right_batch_at(keys, venues, times, values, mutations, 1)
    }

    fn right_batch_at(
        keys: &[String],
        venues: &[&str],
        times: &[i64],
        values: &[&str],
        mutations: &[SourceMutation],
        first_position: u64,
    ) -> RecordBatch {
        let (_, visible) = visible_schemas();
        let rows = RecordBatch::try_new(
            Arc::clone(&visible),
            vec![
                Arc::new(StringArray::from_iter_values(keys)),
                Arc::new(StringArray::from(venues.to_vec())),
                Arc::new(TimestampMillisecondArray::from(times.to_vec())),
                Arc::new(StringArray::from(values.to_vec())),
            ],
        )
        .unwrap();
        let positioned = schema_with_source_row_positions(&visible).unwrap();
        let mutation_schema = schema_with_source_mutations_and_row_positions(&visible).unwrap();
        SourceBatch::positioned(rows, positions(keys.len(), first_position))
            .unwrap()
            .with_mutations(mutations.to_vec())
            .unwrap()
            .into_records_with_metadata(
                SourceRowPositionCapability::OrderedDeterministic,
                &positioned,
                &mutation_schema,
            )
            .unwrap()
    }

    fn key_for_vnode(target: u32) -> String {
        for candidate in 0..1_000 {
            let key = format!("key-{candidate}");
            let batch = left_batch(std::slice::from_ref(&key), &["X"], &[0], &[0]);
            if laminar_core::shuffle::row_vnodes(&batch, &[0, 1], 2).unwrap() == [target] {
                return key;
            }
        }
        panic!("could not find key for vnode {target}");
    }

    fn materialize_capture(capture: StateFrameCapture) -> bytes::Bytes {
        let mut staged_bytes = capture.retained_bytes();
        capture.materialize(&mut staged_bytes, u64::MAX).unwrap()
    }

    fn unaligned_temporal_archive_transport(bytes: &[u8], archive_offset: usize) -> bytes::Bytes {
        let mut transport = vec![0_u8; bytes.len() + CHECKPOINT_ARCHIVE_ALIGNMENT];
        let base = transport.as_ptr() as usize;
        let offset = (0..CHECKPOINT_ARCHIVE_ALIGNMENT)
            .find(|offset| {
                !(base + offset + archive_offset).is_multiple_of(CHECKPOINT_ARCHIVE_ALIGNMENT)
            })
            .expect("a temporal archive transport offset must be unaligned");
        transport[offset..offset + bytes.len()].copy_from_slice(bytes);
        let bytes = bytes::Bytes::from(transport).slice(offset..offset + bytes.len());
        assert_ne!(
            bytes[archive_offset..]
                .as_ptr()
                .align_offset(CHECKPOINT_ARCHIVE_ALIGNMENT),
            0
        );
        bytes
    }

    fn frontier(watermark: i64) -> [InputFrontier; 2] {
        [
            InputFrontier {
                watermark: Some(watermark),
                idle: false,
            },
            InputFrontier {
                watermark: Some(watermark),
                idle: false,
            },
        ]
    }

    #[test]
    fn whole_checkpoint_preflight_bounds_unaligned_owned_vectors() {
        let checkpoint = TemporalJoinOperatorCheckpoint {
            version: OPERATOR_CHECKPOINT_VERSION,
            frontiers: frontier(10).map(Into::into),
            maintenance_cursor: 0,
            maintenance_pending: false,
            maintenance_remaining: 0,
            maintenance_rescan: false,
            published_output_frontier: Some(frontier(10)[0].into()),
            cluster: Some(TemporalClusterCheckpoint {
                assignment_version: 7,
                owner_map_digest: [3; 32],
                self_id: 1,
                local_frontiers: frontier(10).map(Into::into),
                remote_peer_cursors: [None; 2],
                channels: [
                    vec![TemporalCheckpointChannel {
                        peer: 2,
                        applied: frontier(10)[0].into(),
                        events: vec![TemporalCheckpointEvent::Frontier {
                            recovery_gen: 4,
                            frontier: frontier(10)[0].into(),
                        }],
                        positioned_ipc: Vec::new(),
                        mutation_ipc: Vec::new(),
                    }],
                    vec![TemporalCheckpointChannel {
                        peer: 2,
                        applied: frontier(10)[1].into(),
                        events: Vec::new(),
                        positioned_ipc: Vec::new(),
                        mutation_ipc: Vec::new(),
                    }],
                ],
            }),
        };
        let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint).unwrap();
        let encoded = unaligned_temporal_archive_transport(&encoded, 0);
        assert_eq!(checkpoint_alignment_copy_bytes(&encoded), encoded.len());

        let (operator, _, _) = operator(8);
        let preflight = operator
            .preflight_whole_checkpoint_archive(&encoded, "test checkpoint", |_| Ok(()))
            .unwrap();
        let decoded = with_aligned_checkpoint_bytes(&encoded, |bytes| {
            rkyv::from_bytes::<TemporalJoinOperatorCheckpoint, rkyv::rancor::Error>(bytes)
                .map_err(|error| DbError::Checkpoint(error.to_string()))
        })
        .unwrap();
        assert!(
            ManagedTemporalJoinOperator::decoded_whole_checkpoint_bytes(&decoded).unwrap()
                <= preflight.decoded_checkpoint
        );
        assert!(operator
            .preflight_whole_checkpoint_archive(&[0xff, 0xfe, 0xfd], "malformed", |_| Ok(()))
            .is_err());
    }

    #[test]
    fn whole_restore_rejects_preflight_peak_one_byte_over_budget() {
        let (mut donor, _, _) = operator(8);
        donor.frontiers = frontier(10);
        let checkpoint = donor.checkpoint().unwrap().unwrap();
        let (mut restored, _, _) = operator(8);
        let restore_preflight = restored
            .preflight_whole_checkpoint_archive(&checkpoint.data, "test checkpoint", |archived| {
                if archived.cluster.is_some() {
                    return Err(DbError::Checkpoint("unexpected cluster checkpoint".into()));
                }
                Ok(())
            })
            .unwrap();
        let required = restored
            .checked_accounted_state_bytes()
            .unwrap()
            .checked_add(checkpoint_allocation_bytes(checkpoint.data.capacity()).unwrap())
            .and_then(|bytes| {
                bytes.checked_add(checkpoint_alignment_copy_charge(&checkpoint.data).unwrap())
            })
            .and_then(|bytes| bytes.checked_add(restore_preflight.decoded_checkpoint))
            .unwrap();
        restored.set_managed_state_budget(required - 1);

        assert!(matches!(
            restored.restore(checkpoint),
            Err(DbError::ManagedStateBudgetExceeded { .. })
        ));
        assert_eq!(restored.frontiers, [InputFrontier::default(); 2]);
    }

    #[tokio::test]
    async fn local_vnodes_share_one_bounded_path_for_asof_and_tombstones() {
        let key0 = key_for_vnode(0);
        let key1 = key_for_vnode(1);
        let (mut operator, _, _) = operator(1);
        let right = right_batch(
            &[key0.clone(), key0.clone(), key1.clone(), key0.clone()],
            &["X", "X", "X", "Y"],
            &[90, 110, 95, 100],
            &["old", "deleted", "live", "other-venue"],
            &[
                SourceMutation::Put,
                SourceMutation::Tombstone,
                SourceMutation::Put,
                SourceMutation::Put,
            ],
        );
        let left = left_batch(
            &[key0.clone(), key0.clone(), key1, key0],
            &["X", "X", "X", "Y"],
            &[100, 120, 120, 120],
            &[1, 2, 3, 4],
        );
        let fronts = frontier(200);
        let mut output = operator
            .process_with_frontiers(&[vec![left], vec![right]], &fronts)
            .await
            .unwrap();
        assert!(operator.checkpoint_drain_pending());
        let advanced = frontier(250);
        let drained = operator
            .process_with_frontiers(&[], &advanced)
            .await
            .unwrap();
        assert!(drained.iter().map(RecordBatch::num_rows).sum::<usize>() <= 1);
        output.extend(drained);
        while !operator.wants_input() {
            let drained = operator
                .process_with_frontiers(&[], &advanced)
                .await
                .unwrap();
            assert!(drained.iter().map(RecordBatch::num_rows).sum::<usize>() <= 1);
            output.extend(drained);
        }
        assert!(!operator.checkpoint_drain_pending());
        assert_eq!(operator.frontiers, fronts);
        output.extend(
            operator
                .process_with_frontiers(&[], &advanced)
                .await
                .unwrap(),
        );
        assert!(operator.checkpoint_drain_pending());
        while !operator.wants_input() {
            output.extend(
                operator
                    .process_with_frontiers(&[], &advanced)
                    .await
                    .unwrap(),
            );
        }
        assert!(operator.vnode_states.iter().all(Option::is_some));
        assert_eq!(operator.resident_vnodes, [0, 1]);
        assert_eq!(operator.frontiers, advanced);
        assert_eq!(
            operator.output_frontier(InputFrontier {
                watermark: Some(300),
                idle: false,
            }),
            InputFrontier {
                watermark: Some(250),
                idle: false,
            }
        );

        let mut actual = BTreeMap::new();
        for batch in output {
            let ids = batch
                .column(batch.schema().index_of("trade_id").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let values = batch
                .column(batch.schema().index_of("value_quotes").unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                actual.insert(
                    ids.value(row),
                    (!values.is_null(row)).then(|| values.value(row).to_owned()),
                );
            }
        }
        assert_eq!(
            actual,
            BTreeMap::from([
                (1, Some("old".into())),
                (2, None),
                (3, Some("live".into())),
                (4, Some("other-venue".into())),
            ])
        );

        #[cfg(feature = "cluster")]
        {
            let (mut restored_cut, _, _) = self::operator(1);
            restored_cut.frontiers = [
                InputFrontier {
                    watermark: Some(100),
                    idle: true,
                },
                InputFrontier {
                    watermark: Some(50),
                    idle: true,
                },
            ];
            let restored_frontiers = restored_cut.frontiers;
            restored_cut.record_published_output_frontier(&restored_frontiers);
            let checkpoint = restored_cut.checkpoint().unwrap().unwrap();
            let (mut recovered, _, _) = self::operator(1);
            recovered.restore(checkpoint).unwrap();
            assert_eq!(
                recovered.restored_output_frontier(),
                Some(InputFrontier {
                    watermark: Some(100),
                    idle: true,
                })
            );
        }

        let (_, left_schema, right_schema) = self::operator(1);
        let mut negative_config = config();
        negative_config.probe_schedule = TemporalProbeSchedule::list(vec![-50, 0]).unwrap();
        negative_config.probe_alias = Some("probe".into());
        let mut negative = ManagedTemporalJoinOperator::try_new(
            "negative",
            negative_config,
            None,
            SessionContext::new(),
            left_schema,
            right_schema,
            KeyGroupCount::try_from(2_u16).unwrap(),
            limits(1),
        )
        .unwrap();
        negative.frontiers[0] = InputFrontier {
            watermark: Some(100),
            idle: false,
        };
        assert_eq!(
            negative.output_frontier(InputFrontier {
                watermark: Some(100),
                idle: false,
            }),
            InputFrontier {
                watermark: Some(50),
                idle: false,
            }
        );
    }

    #[tokio::test]
    async fn uninitialized_idle_left_holds_output_watermark_until_revival() {
        let key = key_for_vnode(0);
        let (mut operator, _, _) = operator(8);
        let right = right_batch(
            std::slice::from_ref(&key),
            &["X"],
            &[12_000],
            &["live"],
            &[SourceMutation::Put],
        );
        let idle_left = [
            InputFrontier {
                watermark: None,
                idle: true,
            },
            InputFrontier {
                watermark: Some(20_000),
                idle: false,
            },
        ];

        assert!(operator
            .process_with_frontiers(&[Vec::new(), vec![right]], &idle_left)
            .await
            .unwrap()
            .is_empty());
        while !operator.wants_input() {
            assert!(operator
                .process_with_frontiers(&[], &idle_left)
                .await
                .unwrap()
                .is_empty());
        }
        assert_eq!(
            operator
                .published_output_frontier
                .and_then(|frontier| frontier.watermark),
            None
        );

        let left = left_batch(std::slice::from_ref(&key), &["X"], &[15_000], &[7]);
        let revived = [
            InputFrontier {
                watermark: Some(16_000),
                idle: false,
            },
            idle_left[1],
        ];
        let output = operator
            .process_with_frontiers(&[vec![left], Vec::new()], &revived)
            .await
            .unwrap();

        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(
            output[0]
                .column_by_name("value_quotes")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "live"
        );
        assert_eq!(
            operator
                .published_output_frontier
                .and_then(|frontier| frontier.watermark),
            Some(16_000)
        );
    }

    #[tokio::test]
    async fn decision_bound_source_cuts_survive_temporal_checkpoint_round_trip() {
        let decision_cut = [
            InputFrontier {
                watermark: Some(900),
                idle: false,
            },
            InputFrontier {
                watermark: Some(700),
                idle: false,
            },
        ];
        let (mut operator, _, _) = operator(8);

        operator
            .process_with_frontiers(&[], &decision_cut)
            .await
            .unwrap();
        for _ in 0..32 {
            if operator.wants_input() {
                break;
            }
            operator
                .process_with_frontiers(&[], &decision_cut)
                .await
                .unwrap();
        }
        assert!(
            operator.wants_input(),
            "temporal frontier drain did not settle"
        );
        assert_eq!(
            operator.published_output_frontier,
            Some(InputFrontier {
                watermark: Some(700),
                idle: false,
            }),
            "the higher left decision must not publish beyond the lower right decision"
        );

        let checkpoint = operator
            .checkpoint()
            .unwrap()
            .expect("an initialized temporal frontier must be checkpointed");
        let (mut restored, _, _) = self::operator(8);
        restored.restore(checkpoint).unwrap();
        assert_eq!(
            restored.published_output_frontier,
            Some(InputFrontier {
                watermark: Some(700),
                idle: false,
            })
        );

        restored
            .process_with_frontiers(&[], &decision_cut)
            .await
            .unwrap();
        for _ in 0..32 {
            if restored.wants_input() {
                break;
            }
            restored
                .process_with_frontiers(&[], &decision_cut)
                .await
                .unwrap();
        }
        assert!(
            restored.wants_input(),
            "restored frontier drain did not settle"
        );
        assert_eq!(
            restored.published_output_frontier,
            Some(InputFrontier {
                watermark: Some(700),
                idle: false,
            })
        );
        assert!(restored.checkpoint().unwrap().is_some());
    }

    #[tokio::test]
    async fn projection_sees_only_visible_join_columns() {
        let keys = [key_for_vnode(0), key_for_vnode(1)];
        let (mut operator, _, _) = operator_with_projection(
            8,
            Some("SELECT * FROM __temporal_tmp WHERE value_quotes = 'live'"),
        );
        operator.initialize_managed_state().await.unwrap();
        let left = left_batch(&keys, &["X", "X"], &[100, 100], &[7, 8]);
        let right = right_batch(
            &keys,
            &["X", "X"],
            &[90, 90],
            &["live", "stale"],
            &[SourceMutation::Put, SourceMutation::Put],
        );

        let frontiers = frontier(200);
        let mut output = operator
            .process_with_frontiers(&[vec![left], vec![right]], &frontiers)
            .await
            .unwrap();
        while !operator.wants_input() {
            output.extend(
                operator
                    .process_with_frontiers(&[], &frontiers)
                    .await
                    .unwrap(),
            );
        }

        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0]
                .column_by_name("trade_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
        assert_eq!(
            output[0]
                .column_by_name("value_quotes")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "live"
        );
        for hidden in [
            SOURCE_PARTITION_COLUMN,
            SOURCE_ORDER_KEY_COLUMN,
            SOURCE_SUB_OFFSET_COLUMN,
        ] {
            assert!(output[0].column_by_name(hidden).is_none());
        }
    }

    #[tokio::test]
    async fn invalid_post_projection_fails_initialization_before_state_admission() {
        let (mut operator, _, _) =
            operator_with_projection(8, Some("SELECT missing_column FROM __temporal_tmp"));

        let error = operator.initialize_managed_state().await.unwrap_err();

        assert!(matches!(error, DbError::Pipeline(_)));
        assert!(operator.vnode_states.iter().all(Option::is_none));
    }

    #[tokio::test]
    async fn vnode_capture_is_full_then_sparse_and_forceable() {
        let key = key_for_vnode(1);
        let (mut donor, _, _) = operator(8);
        let right = right_batch(
            std::slice::from_ref(&key),
            &["X"],
            &[90],
            &["live"],
            &[SourceMutation::Put],
        );
        donor
            .process_with_frontiers(&[Vec::new(), vec![right]], &[InputFrontier::default(); 2])
            .await
            .unwrap();
        donor.maintenance_cursor = 1;
        let whole = donor.checkpoint().unwrap().unwrap();
        let captured = donor
            .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|frame| (frame.vnode, frame.state.map(materialize_capture)))
            .collect::<Vec<_>>();
        assert_eq!(captured.len(), 2);
        assert_eq!(
            captured.iter().map(|(vnode, _)| *vnode).collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(captured[0].1.as_deref().unwrap(), &[ABSENT_VNODE]);
        assert_eq!(captured[1].1.as_deref().unwrap()[0], PRESENT_VNODE);
        let clean = donor
            .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
            .unwrap()
            .unwrap();
        assert!(clean.is_empty());

        let update = right_batch_at(
            std::slice::from_ref(&key),
            &["X"],
            &[95],
            &["new"],
            &[SourceMutation::Put],
            2,
        );
        donor
            .process_with_frontiers(&[Vec::new(), vec![update]], &[InputFrontier::default(); 2])
            .await
            .unwrap();
        let sparse = donor
            .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(sparse.len(), 1);
        assert_eq!(sparse[0].vnode, 1);
        assert!(sparse[0].state.is_some());

        let (mut restored, _, _) = operator(8);
        restored.restore(whole).unwrap();
        assert_eq!(restored.maintenance_cursor, 1);
        restored
            .restore_vnode(0, 2, captured[0].1.as_deref().unwrap())
            .unwrap();
        let present = unaligned_temporal_archive_transport(captured[1].1.as_deref().unwrap(), 1);
        assert_eq!(
            vnode_checkpoint_alignment_copy_bytes(&present),
            present.len() - 1
        );
        let restore_transport_peak = restored
            .checked_accounted_state_bytes()
            .unwrap()
            .checked_add(present.len())
            .and_then(|bytes| {
                bytes.checked_add(vnode_checkpoint_alignment_copy_charge(&present).unwrap())
            })
            .unwrap();
        restored.set_managed_state_budget(restore_transport_peak - 1);
        assert!(matches!(
            restored.restore_vnode(1, 2, &present),
            Err(DbError::ManagedStateBudgetExceeded { .. })
        ));
        assert!(restored.vnode_states[1].is_none());
        restored.set_managed_state_budget(usize::MAX);
        restored.restore_vnode(1, 2, &present).unwrap();
        assert!(restored.vnode_states[0].is_none());
        assert!(restored.vnode_states[1].is_some());
        restored.force_full_vnode_capture();
        let recaptured = restored
            .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(recaptured.len(), 2);
        assert_eq!(
            recaptured
                .iter()
                .map(|frame| frame.vnode)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert!(recaptured.iter().all(|frame| frame.state.is_some()));

        let left = left_batch(std::slice::from_ref(&key), &["X"], &[100], &[7]);
        let output = restored
            .process_with_frontiers(&[vec![left], Vec::new()], &frontier(200))
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let value = output[0]
            .column(output[0].schema().index_of("value_quotes").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(value.value(0), "live");
    }

    #[cfg(feature = "cluster")]
    async fn two_owner_scope() -> ClusterShuffleConfig {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::cluster::control::LeaseDeadline;
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};

        let registry = Arc::new(VnodeRegistry::new(2));
        registry.set_assignment(vec![NodeId(1), NodeId(2)].into());
        let receiver = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(1, uuid::Uuid::from_u128(1)));
        let deadline = Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(60)));
        receiver
            .install_process_lease_deadline(Arc::clone(&deadline))
            .unwrap();
        sender.install_process_lease_deadline(deadline).unwrap();
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
        sender.install_assignment_fence(&fence, &[1, 2]).unwrap();
        receiver.install_assignment_fence(&fence, &[1, 2]).unwrap();
        ClusterShuffleConfig {
            registry,
            sender,
            receiver,
            self_id: NodeId(1),
        }
    }

    #[cfg(feature = "cluster")]
    fn install_all_local_assignment(scope: &ClusterShuffleConfig) -> CheckpointAssignmentFence {
        use laminar_core::checkpoint::CheckpointParticipant;

        let version = scope.registry.assignment_version() + 1;
        let owners = [1_u64, 1];
        let fence = CheckpointAssignmentFence::from_owner_map(
            version,
            &owners,
            vec![CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            }],
        )
        .unwrap();
        scope
            .sender
            .install_assignment_fence(&fence, &owners)
            .unwrap();
        scope
            .receiver
            .install_assignment_fence(&fence, &owners)
            .unwrap();
        scope
            .registry
            .set_assignment_and_version(Arc::from(owners.map(NodeId)), version);
        fence
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn fresh_owner_installs_portable_cut_and_bootstrap_topology() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

        use crate::operator_graph::{ManagedVnodeRestore, ManagedWholeRestore};

        let key = key_for_vnode(0);
        let cut = frontier(100);
        let (mut donor, _, _) = operator(8);
        donor
            .process_with_frontiers(
                &[
                    Vec::new(),
                    vec![right_batch(
                        std::slice::from_ref(&key),
                        &["X"],
                        &[90],
                        &["live"],
                        &[SourceMutation::Put],
                    )],
                ],
                &cut,
            )
            .await
            .unwrap();
        while !donor.wants_input() {
            donor.process_with_frontiers(&[], &cut).await.unwrap();
        }
        let captured = donor.checkpoint_vnodes(&[0], 2, u64::MAX).unwrap().unwrap();
        let vnode_frame = materialize_capture(captured.into_iter().next().unwrap().state.unwrap());
        let vnode_frame = unaligned_temporal_archive_transport(&vnode_frame, 1);

        let scope = two_owner_scope().await;
        let target_version = scope.registry.assignment_version() + 1;
        let target_fence = CheckpointAssignmentFence::from_owner_map(
            target_version,
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
        scope
            .sender
            .install_assignment_fence(&target_fence, &[1, 2])
            .unwrap();
        scope
            .receiver
            .install_assignment_fence(&target_fence, &[1, 2])
            .unwrap();
        scope
            .registry
            .set_assignment_and_version(Arc::from([NodeId(1), NodeId(2)]), target_version);

        let predecessor = CheckpointAssignmentFence::from_owner_map(
            target_version - 1,
            &[2, 3],
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
        let predecessor_registry = VnodeRegistry::new(2);
        predecessor_registry.set_assignment_and_version(
            Arc::from([NodeId(2), NodeId(3)]),
            predecessor.assignment_version,
        );

        let (mut target, _, _) = operator(8);
        target.attach_cluster_shuffle(scope.clone());
        target.local_assignment = predecessor_registry.versioned_snapshot();
        target.cluster_peers = Arc::from([2_u64, 3]);
        for port in 0..2 {
            target.peer_channels[port].entry(3).or_default();
        }

        let encode_whole =
            |participant_id: u64, peer: u64, queued: bool, published_watermark: i64| {
                let channel = |side: usize| TemporalCheckpointChannel {
                    peer,
                    applied: cut[side].into(),
                    events: if queued && side == 0 {
                        vec![TemporalCheckpointEvent::Frontier {
                            recovery_gen: 0,
                            frontier: cut[side].into(),
                        }]
                    } else {
                        Vec::new()
                    },
                    positioned_ipc: Vec::new(),
                    mutation_ipc: Vec::new(),
                };
                let checkpoint = TemporalJoinOperatorCheckpoint {
                    version: OPERATOR_CHECKPOINT_VERSION,
                    frontiers: cut.map(Into::into),
                    maintenance_cursor: 0,
                    maintenance_pending: false,
                    maintenance_remaining: 0,
                    maintenance_rescan: false,
                    published_output_frontier: Some(
                        InputFrontier {
                            watermark: Some(published_watermark),
                            idle: participant_id == 3,
                        }
                        .into(),
                    ),
                    cluster: Some(TemporalClusterCheckpoint {
                        assignment_version: predecessor.assignment_version,
                        owner_map_digest: predecessor.assignment_digest,
                        self_id: participant_id,
                        local_frontiers: cut.map(Into::into),
                        remote_peer_cursors: [None; 2],
                        channels: [vec![channel(0)], vec![channel(1)]],
                    }),
                };
                rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
                    .unwrap()
                    .to_vec()
            };
        let restores = [ManagedVnodeRestore {
            participant_id: 2,
            vnode: 0,
            state: vnode_frame.as_ref(),
        }];

        let absent_vnode = [ABSENT_VNODE];
        let multi_donor_restores = [
            ManagedVnodeRestore {
                participant_id: 2,
                vnode: 0,
                state: vnode_frame.as_ref(),
            },
            ManagedVnodeRestore {
                participant_id: 3,
                vnode: 1,
                state: &absent_vnode,
            },
        ];
        let donor2_frame = encode_whole(2, 3, false, 80);
        let donor3_frame = encode_whole(3, 2, false, 60);
        let multi_donor_whole = [
            ManagedWholeRestore {
                participant_id: 2,
                state: &donor2_frame,
            },
            ManagedWholeRestore {
                participant_id: 3,
                state: &donor3_frame,
            },
        ];
        let merged = target
            .portable_handoff_cut(
                &ManagedVnodeTransition {
                    predecessor: &predecessor,
                    target: &target_fence,
                    revoked: &rustc_hash::FxHashSet::default(),
                    restores: &multi_donor_restores,
                    whole_restores: &multi_donor_whole,
                    mode: ManagedVnodeTransitionMode::Live,
                },
                true,
            )
            .unwrap()
            .unwrap();
        assert_eq!(merged.frontiers, cut);
        assert_eq!(
            merged.published_output_frontier,
            Some(InputFrontier {
                watermark: Some(60),
                idle: false,
            })
        );

        let queued_frame = encode_whole(2, 3, true, 100);
        let queued_whole = [ManagedWholeRestore {
            participant_id: 2,
            state: &queued_frame,
        }];
        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &queued_whole,
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap_err();
        assert_eq!(target.frontiers, [InputFrontier::default(); 2]);
        assert!(target.vnode_states.iter().all(Option::is_none));

        let whole_frame = encode_whole(2, 3, false, 100);
        let whole_frame = unaligned_temporal_archive_transport(&whole_frame, 0);
        let whole_restores = [ManagedWholeRestore {
            participant_id: 2,
            state: &whole_frame,
        }];
        let decoded_bound = target
            .preflight_whole_checkpoint_archive(&whole_frame, "test donor", |_| Ok(()))
            .unwrap()
            .decoded_checkpoint;
        let raw_restore_bytes = vnode_frame.len().checked_add(whole_frame.len()).unwrap();
        let sequential_peak = vnode_checkpoint_alignment_copy_charge(&vnode_frame)
            .unwrap()
            .max(
                checkpoint_alignment_copy_charge(&whole_frame)
                    .unwrap()
                    .checked_add(decoded_bound)
                    .unwrap(),
            );
        let restore_peak = target
            .checked_accounted_state_bytes()
            .unwrap()
            .checked_add(raw_restore_bytes)
            .and_then(|bytes| bytes.checked_add(sequential_peak))
            .unwrap();
        target.set_managed_state_budget(restore_peak - 1);
        assert!(matches!(
            target.prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &whole_restores,
                mode: ManagedVnodeTransitionMode::Live,
            }),
            Err(DbError::ManagedStateBudgetExceeded { .. })
        ));
        assert!(target.prepared_vnode_transition.is_none());
        target.set_managed_state_budget(usize::MAX);
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
        assert_eq!(target.frontiers, [InputFrontier::default(); 2]);
        target.publish_vnode_transition();
        assert_eq!(target.frontiers, cut);
        assert_eq!(target.local_frontiers, cut);
        assert_eq!(target.published_output_frontier, Some(cut[0]));
        assert_eq!(target.cluster_peers.as_ref(), &[2]);
        for channel in target.peer_channels.iter().flat_map(BTreeMap::values) {
            assert_eq!(channel.applied, cut[0]);
            assert_eq!(channel.accepted, cut[0]);
            assert!(channel.events.is_empty());
        }
        assert_eq!(target.last_broadcasts, [InputFrontier::default(); 2]);
        assert!(target.checkpoint_drain_pending());
        target.finish_vnode_transition();

        let predecessor_owners = [NodeId(2), NodeId(3)];
        let (mut bootstrap, _, _) = operator(8);
        bootstrap.attach_cluster_shuffle(scope);
        bootstrap
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &whole_restores,
                mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
                    predecessor_owners: &predecessor_owners,
                },
            })
            .unwrap();
        bootstrap.publish_vnode_transition();
        assert_eq!(bootstrap.frontiers, cut);
        assert_eq!(bootstrap.local_frontiers, cut);
        assert_eq!(bootstrap.published_output_frontier, Some(cut[0]));
        assert_eq!(
            bootstrap.vnode_states[0]
                .as_ref()
                .unwrap()
                .retained_versions(),
            1
        );
        bootstrap.finish_vnode_transition();
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn zero_owner_transition_publishes_remote_topology_without_state() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

        let scope = two_owner_scope().await;
        let predecessor_version = scope.registry.assignment_version();
        let target_version = predecessor_version + 1;
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            predecessor_version,
            &[2, 2],
            vec![CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            }],
        )
        .unwrap();
        let target = CheckpointAssignmentFence::from_owner_map(
            target_version,
            &[3, 3],
            vec![CheckpointParticipant {
                node_id: 3,
                boot_incarnation: uuid::Uuid::from_u128(3),
            }],
        )
        .unwrap();

        let (mut operator, _, _) = operator(8);
        operator.attach_cluster_shuffle(scope.clone());
        let predecessor_registry = VnodeRegistry::new_unassigned(2);
        predecessor_registry
            .set_assignment_and_version(Arc::from([NodeId(2), NodeId(2)]), predecessor_version);
        operator.local_assignment = predecessor_registry.versioned_snapshot();

        scope
            .registry
            .set_assignment_and_version(Arc::from([NodeId(3), NodeId(3)]), target_version);
        scope.sender.invalidate_assignment_fence();
        scope.receiver.invalidate_assignment_fence();

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
        assert_eq!(operator.local_assignment.version(), predecessor_version);
        assert_eq!(operator.cluster_peers.as_ref(), &[2]);
        assert!(operator.vnode_states.iter().all(Option::is_none));

        operator.publish_vnode_transition();

        assert_eq!(operator.local_assignment.version(), target_version);
        assert_eq!(operator.local_assignment.owners(), &[NodeId(3), NodeId(3)]);
        assert_eq!(operator.cluster_peers.as_ref(), &[3]);
        assert!(operator
            .peer_channels
            .iter()
            .all(|channels| { channels.len() == 1 && channels.contains_key(&3) }));
        assert!(operator.resident_vnodes.is_empty());
        assert!(operator.vnode_states.iter().all(Option::is_none));
        operator.finish_vnode_transition();
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn vnode_transition_is_atomic_and_publishes_target_topology() {
        use laminar_core::checkpoint::CheckpointParticipant;
        use laminar_core::shuffle::ShuffleMessage;

        use crate::operator_graph::ManagedVnodeRestore;

        let key = key_for_vnode(1);
        let cut = frontier(100);
        let (mut donor, _, _) = operator(8);
        donor
            .process_with_frontiers(
                &[
                    Vec::new(),
                    vec![right_batch(
                        std::slice::from_ref(&key),
                        &["X"],
                        &[90],
                        &["live"],
                        &[SourceMutation::Put],
                    )],
                ],
                &cut,
            )
            .await
            .unwrap();
        while !donor.wants_input() {
            donor.process_with_frontiers(&[], &cut).await.unwrap();
        }
        let captured = donor.checkpoint_vnodes(&[1], 2, u64::MAX).unwrap().unwrap();
        let frame = materialize_capture(captured.into_iter().next().unwrap().state.unwrap());

        let (mut target, _, _) = operator(8);
        let scope = two_owner_scope().await;
        target.attach_cluster_shuffle(scope.clone());
        target.frontiers = cut;
        target.local_frontiers = cut;
        target.last_broadcasts = cut;
        for channel in target
            .peer_channels
            .iter_mut()
            .flat_map(BTreeMap::values_mut)
        {
            channel.applied = cut[0];
            channel.accepted = cut[0];
        }
        let predecessor_version = target.local_assignment.version();
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            predecessor_version,
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
        let fence = install_all_local_assignment(&scope);
        let corrupt = [PRESENT_VNODE, 0xff];
        let corrupt_restore = [ManagedVnodeRestore {
            participant_id: 2,
            vnode: 1,
            state: &corrupt,
        }];
        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &corrupt_restore,
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap_err();
        assert_eq!(target.local_assignment.version(), predecessor_version);
        assert!(target.vnode_states.iter().all(Option::is_none));
        assert_eq!(target.cluster_peers.as_ref(), &[2]);

        let restores = [ManagedVnodeRestore {
            participant_id: 2,
            vnode: 1,
            state: frame.as_ref(),
        }];
        let send = tokio::spawn(std::future::pending::<(
            Result<(), DbError>,
            Option<Vec<(u64, ShuffleMessage)>>,
        )>());
        target.pending_cluster_input = Some(PendingTemporalClusterInput {
            routed: BTreeMap::new(),
            outbound: None,
            local_frontiers: cut,
            send: Some(send),
            accounted_bytes: 0,
        });
        let error = target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap_err();
        assert!(error.to_string().contains("drained frontier"));
        target.pending_cluster_input.take();

        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        assert!(target.managed_state_accounting().unwrap().prepared > 0);
        target.abort_vnode_transition();
        assert!(target.vnode_states.iter().all(Option::is_none));
        target.finish_vnode_transition();

        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        target.publish_vnode_transition();
        assert_eq!(target.local_assignment.version(), fence.assignment_version);
        assert_eq!(target.resident_vnodes, [1]);
        assert_eq!(
            target.vnode_states[1].as_ref().unwrap().retained_versions(),
            1
        );
        assert!(target.cluster_peers.is_empty());
        assert!(target.managed_state_accounting().unwrap().retired > 0);
        target.finish_vnode_transition();
        assert!(!target.checkpoint_drain_pending());
        assert!(target.wants_input());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_plan_is_atomic_and_orders_idle_revival_data_and_frontier() {
        use laminar_core::shuffle::ShuffleMessage;

        let key = key_for_vnode(1);
        let (mut operator, _, _) = operator(8);
        let scope = two_owner_scope().await;
        operator.attach_cluster_shuffle(scope.clone());
        let idle = InputFrontier {
            watermark: Some(100),
            idle: true,
        };
        operator.local_frontiers = [idle; 2];
        operator.frontiers = [InputFrontier {
            watermark: Some(200),
            idle: false,
        }; 2];
        for port in 0..2 {
            let channel = operator.peer_channels[port].get_mut(&2).unwrap();
            channel.applied = idle;
            channel.accepted = idle;
        }
        operator.last_broadcasts = [idle; 2];
        let left = left_batch(std::slice::from_ref(&key), &["X"], &[210], &[7]);
        let right = right_batch(
            std::slice::from_ref(&key),
            &["X"],
            &[205],
            &["live"],
            &[SourceMutation::Put],
        );
        let active = [InputFrontier {
            watermark: Some(150),
            idle: false,
        }; 2];
        let assignment = scope.registry.versioned_snapshot();
        let plan = operator
            .plan_cluster_inputs(
                &[vec![left], vec![right]],
                active,
                &scope,
                &assignment,
                &[2],
            )
            .unwrap();
        assert_eq!(plan.outbound.len(), 6);
        let expected = [
            ("temporal::right", "frontier", Some(100)),
            ("temporal::right", "data", None),
            ("temporal::right", "frontier", Some(200)),
            ("temporal::left", "frontier", Some(100)),
            ("temporal::left", "data", None),
            ("temporal::left", "frontier", Some(200)),
        ];
        for ((peer, message), (stage, kind, watermark)) in plan.outbound.iter().zip(expected) {
            assert_eq!(*peer, 2);
            match (kind, message) {
                (
                    "frontier",
                    ShuffleMessage::Frontier {
                        stage: actual,
                        watermark: actual_watermark,
                        idle: false,
                    },
                ) => {
                    assert_eq!(actual, stage);
                    assert_eq!(*actual_watermark, watermark);
                }
                ("data", ShuffleMessage::Data { stage: actual, .. }) => {
                    assert_eq!(actual, stage);
                }
                _ => panic!("unexpected temporal shuffle order"),
            }
        }
        operator.local_frontiers = plan.local_frontiers;
        operator.last_broadcasts = [idle; 2];
        let revived_without_data = operator
            .plan_cluster_inputs(
                &[Vec::new(), Vec::new()],
                operator.local_frontiers,
                &scope,
                &assignment,
                &[2],
            )
            .unwrap();
        assert_eq!(revived_without_data.outbound.len(), 2);
        assert!(revived_without_data.outbound.iter().all(|(_, message)| {
            matches!(
                message,
                ShuffleMessage::Frontier {
                    watermark: Some(200),
                    idle: false,
                    ..
                }
            )
        }));
        operator.last_broadcasts = revived_without_data.local_frontiers;
        let unchanged = operator
            .plan_cluster_inputs(
                &[Vec::new(), Vec::new()],
                plan.local_frontiers,
                &scope,
                &assignment,
                &[2],
            )
            .unwrap();
        assert!(unchanged.outbound.is_empty());

        let invalid_left = RecordBatch::new_empty(Arc::new(Schema::empty()));
        assert!(operator
            .plan_cluster_inputs(
                &[vec![invalid_left], Vec::new()],
                plan.local_frontiers,
                &scope,
                &assignment,
                &[2],
            )
            .is_err());

        operator.local_frontiers[0] = InputFrontier {
            watermark: Some(300),
            idle: false,
        };
        operator.frontiers[0] = InputFrontier {
            watermark: Some(100),
            idle: false,
        };
        let late = left_batch(std::slice::from_ref(&key), &["X"], &[250], &[8]);
        assert!(operator
            .plan_cluster_inputs(
                &[vec![late], Vec::new()],
                operator.local_frontiers,
                &scope,
                &assignment,
                &[2],
            )
            .is_err());

        let nullable = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("venue", DataType::Utf8, false),
                Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["X"])),
                Arc::new(TimestampMillisecondArray::from(vec![None])),
            ],
        )
        .unwrap();
        assert!(operator
            .validate_batch_lateness(
                TemporalInputSide::Left,
                &nullable,
                operator.local_frontiers[0],
                false,
            )
            .is_err());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn bootstrap_broadcast_holds_restored_cut_ahead_of_live_replay_frontier() {
        use laminar_core::shuffle::ShuffleMessage;

        let key = key_for_vnode(1);
        let (mut operator, _, _) = operator(8);
        let scope = two_owner_scope().await;
        operator.attach_cluster_shuffle(scope.clone());
        let cut = InputFrontier {
            watermark: Some(100),
            idle: false,
        };
        let live = InputFrontier {
            watermark: Some(300),
            idle: false,
        };
        operator.frontiers = [cut; 2];
        operator.local_frontiers = [cut; 2];
        operator.last_broadcasts = [InputFrontier::default(); 2];
        for channels in &mut operator.peer_channels {
            let channel = channels.get_mut(&2).unwrap();
            channel.applied = cut;
            channel.accepted = cut;
        }
        let assignment = scope.registry.versioned_snapshot();

        assert!(!operator.wants_input());
        let bootstrap = operator
            .plan_cluster_inputs(
                &[Vec::new(), Vec::new()],
                [live; 2],
                &scope,
                &assignment,
                &[2],
            )
            .unwrap();
        assert_eq!(bootstrap.local_frontiers, [cut; 2]);
        assert_eq!(bootstrap.outbound.len(), 2);
        assert!(bootstrap.outbound.iter().all(|(_, message)| {
            matches!(
                message,
                ShuffleMessage::Frontier {
                    watermark: Some(100),
                    idle: false,
                    ..
                }
            )
        }));

        let replay = right_batch(
            std::slice::from_ref(&key),
            &["X"],
            &[150],
            &["replayed"],
            &[SourceMutation::Put],
        );
        let error = match operator.plan_cluster_inputs(
            &[Vec::new(), vec![replay.clone()]],
            [live; 2],
            &scope,
            &assignment,
            &[2],
        ) {
            Err(error) => error,
            Ok(_) => panic!("local replay must wait for the restored frontier broadcast"),
        };
        assert!(
            error
                .to_string()
                .contains("before its restored frontier was broadcast"),
            "{error}"
        );

        operator.last_broadcasts = [cut; 2];
        let replay = operator
            .plan_cluster_inputs(
                &[Vec::new(), vec![replay]],
                [live; 2],
                &scope,
                &assignment,
                &[2],
            )
            .unwrap();
        assert_eq!(replay.local_frontiers, [live; 2]);
        assert_eq!(replay.outbound.len(), 3);
        assert!(matches!(
            &replay.outbound[0].1,
            ShuffleMessage::Data { stage, .. } if stage == "temporal::right"
        ));
        assert!(matches!(
            &replay.outbound[1].1,
            ShuffleMessage::Frontier {
                stage,
                watermark: Some(300),
                idle: false,
            } if stage == "temporal::right"
        ));
        assert!(matches!(
            &replay.outbound[2].1,
            ShuffleMessage::Frontier {
                stage,
                watermark: Some(300),
                idle: false,
            } if stage == "temporal::left"
        ));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn zero_admission_send_restarts_once_without_becoming_runnable() {
        use laminar_core::shuffle::ShuffleMessage;

        let (mut operator, _, _) = operator(8);
        let scope = two_owner_scope().await;
        operator.initialize_managed_state().await.unwrap();
        operator.attach_cluster_shuffle(scope);

        let retry_plan = vec![(
            2,
            ShuffleMessage::Frontier {
                stage: "temporal::left".to_owned(),
                watermark: None,
                idle: false,
            },
        )];
        let send = tokio::spawn(async move {
            (
                Err(DbError::ShuffleNotReady("injected zero admission".into())),
                Some(retry_plan),
            )
        });
        operator.pending_cluster_input = Some(PendingTemporalClusterInput {
            routed: BTreeMap::new(),
            outbound: None,
            local_frontiers: [InputFrontier::default(); 2],
            send: Some(send),
            accounted_bytes: 0,
        });

        while !operator
            .pending_cluster_input
            .as_ref()
            .unwrap()
            .send
            .as_ref()
            .unwrap()
            .is_finished()
        {
            tokio::task::yield_now().await;
        }
        assert!(!operator.deferred_work_is_runnable());

        let output = operator
            .process_cluster(&[Vec::new(), Vec::new()], [InputFrontier::default(); 2])
            .await
            .unwrap();
        assert!(output.is_empty());
        let pending = operator.pending_cluster_input.as_ref().unwrap();
        assert!(pending.send.is_some());
        assert!(pending.outbound.is_none());
        assert!(!operator.deferred_work_is_runnable());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn pending_send_drains_remote_history_before_local_probe() {
        use laminar_core::shuffle::ShuffleMessage;

        let local_key = key_for_vnode(0);
        let outbound_key = key_for_vnode(1);
        let (mut operator, _, _) = operator(8);
        let scope = two_owner_scope().await;
        operator.initialize_managed_state().await.unwrap();
        operator.attach_cluster_shuffle(scope.clone());
        let close = frontier(300);
        let assignment = scope.registry.versioned_snapshot();
        let plan = operator
            .plan_cluster_inputs(
                &[
                    vec![
                        left_batch(std::slice::from_ref(&local_key), &["X"], &[220], &[7]),
                        left_batch(std::slice::from_ref(&outbound_key), &["X"], &[220], &[8]),
                    ],
                    Vec::new(),
                ],
                close,
                &scope,
                &assignment,
                &[2],
            )
            .unwrap();
        assert!(plan.routed.contains_key(&0));
        assert!(plan
            .outbound
            .iter()
            .any(|(_, message)| matches!(message, ShuffleMessage::Data { .. })));
        let accounted_bytes = operator.cluster_input_plan_bytes(&plan).unwrap();
        let ClusterInputPlan {
            routed,
            outbound,
            local_frontiers,
            effective_frontiers: _,
        } = plan;
        let baseline = operator.managed_state_accounting().unwrap().live;
        let (release, wait) = tokio::sync::oneshot::channel();
        let send = tokio::spawn(async move {
            let _ = wait.await;
            drop(outbound);
            (Ok(()), None)
        });
        operator.pending_cluster_input = Some(PendingTemporalClusterInput {
            routed,
            outbound: None,
            local_frontiers,
            send: Some(send),
            accounted_bytes,
        });

        let assignment_version = scope.registry.assignment_version();
        let recovery_gen = scope.receiver.recovery_gen();
        operator
            .stage_checkpointed_shuffle(
                "temporal::right",
                crate::operator::RetainedBatch::restored_channel(
                    right_batch(
                        std::slice::from_ref(&local_key),
                        &["X"],
                        &[210],
                        &["live"],
                        &[SourceMutation::Put],
                    ),
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
                "temporal::right",
                2,
                close[1],
                assignment_version,
                recovery_gen,
            )
            .unwrap();
        operator
            .stage_checkpointed_shuffle_frontier(
                "temporal::left",
                2,
                close[0],
                assignment_version,
                recovery_gen,
            )
            .unwrap();
        assert!(operator.deferred_work_is_runnable());
        assert!(operator.managed_state_accounting().unwrap().live >= baseline + accounted_bytes);

        let output = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            operator.process_cluster(&[Vec::new(), Vec::new()], [InputFrontier::default(); 2]),
        )
        .await
        .expect("pending temporal send blocked the graph task")
        .unwrap();
        assert!(output.is_empty());
        assert_eq!(operator.queued_remote_events, 0);
        let local_state = operator.vnode_states[0].as_ref().unwrap();
        assert_eq!(local_state.retained_versions(), 1);
        assert_eq!(local_state.pending_probes(), 0);
        assert_eq!(operator.frontiers, [InputFrontier::default(); 2]);
        assert_eq!(operator.local_frontiers, [InputFrontier::default(); 2]);
        assert_eq!(operator.last_broadcasts, [InputFrontier::default(); 2]);
        assert!(operator.pending_cluster_input.is_some());
        assert!(!operator.wants_input());
        assert!(!operator.deferred_work_is_runnable());
        assert!(operator.capture_operator_checkpoint(usize::MAX).is_err());
        assert!(operator.checkpoint_vnodes(&[0], 2, u64::MAX).is_err());

        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !operator
                .pending_cluster_input
                .as_ref()
                .unwrap()
                .send
                .as_ref()
                .unwrap()
                .is_finished()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending temporal send task did not finish");
        let output = operator
            .process_cluster(&[Vec::new(), Vec::new()], [InputFrontier::default(); 2])
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let ids = output[0]
            .column_by_name("trade_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = output[0]
            .column_by_name("value_quotes")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), 7);
        assert_eq!(values.value(0), "live");
        assert!(operator.pending_cluster_input.is_none());
        assert_eq!(operator.local_frontiers, close);
        assert_eq!(operator.last_broadcasts, close);
        assert_eq!(operator.frontiers, close);

        let output = operator
            .process_cluster(&[Vec::new(), Vec::new()], close)
            .await
            .unwrap();
        assert!(output.is_empty());
        assert!(operator.wants_input());
        assert!(!operator.checkpoint_drain_pending());
    }

    #[cfg(feature = "cluster")]
    async fn queued_cluster_checkpoint() -> (OperatorCheckpoint, ClusterShuffleConfig) {
        let key = key_for_vnode(0);
        let (mut operator, _, _) = operator(8);
        let scope = two_owner_scope().await;
        operator.attach_cluster_shuffle(scope.clone());
        let applied = InputFrontier {
            watermark: Some(100),
            idle: false,
        };
        let local = InputFrontier {
            watermark: Some(300),
            idle: false,
        };
        operator.frontiers = [applied; 2];
        operator.local_frontiers = [local; 2];
        operator.last_broadcasts = [local; 2];
        operator.published_output_frontier = Some(InputFrontier {
            watermark: Some(100),
            idle: true,
        });
        for port in 0..2 {
            let channel = operator.peer_channels[port].get_mut(&2).unwrap();
            channel.applied = applied;
            channel.accepted = applied;
        }

        let assignment = scope.registry.assignment_version();
        let recovery = scope.receiver.recovery_gen();
        let stale_right = crate::operator::RetainedBatch::restored_channel(
            right_batch(
                std::slice::from_ref(&key),
                &["X"],
                &[205],
                &["stale"],
                &[SourceMutation::Put],
            ),
            2,
            assignment,
            recovery,
            Arc::from([0_u32]),
        );
        operator
            .stage_checkpointed_shuffle("temporal::right", stale_right, 100)
            .unwrap();
        let removed_right = crate::operator::RetainedBatch::restored_channel(
            right_batch_at(
                std::slice::from_ref(&key),
                &["X"],
                &[207],
                &["deleted"],
                &[SourceMutation::Tombstone],
                2,
            ),
            2,
            assignment,
            recovery,
            Arc::from([0_u32]),
        );
        operator
            .stage_checkpointed_shuffle("temporal::right", removed_right, 100)
            .unwrap();
        let right = crate::operator::RetainedBatch::restored_channel(
            right_batch_at(
                std::slice::from_ref(&key),
                &["X"],
                &[210],
                &["live"],
                &[SourceMutation::Put],
                3,
            ),
            2,
            assignment,
            recovery,
            Arc::from([0_u32]),
        );
        operator
            .stage_checkpointed_shuffle("temporal::right", right, 100)
            .unwrap();
        operator
            .stage_checkpointed_shuffle_frontier(
                "temporal::right",
                2,
                InputFrontier {
                    watermark: Some(250),
                    idle: false,
                },
                assignment,
                recovery,
            )
            .unwrap();
        let left = crate::operator::RetainedBatch::restored_channel(
            left_batch(std::slice::from_ref(&key), &["X"], &[220], &[7]),
            2,
            assignment,
            recovery,
            Arc::from([0_u32]),
        );
        operator
            .stage_checkpointed_shuffle("temporal::left", left, 100)
            .unwrap();
        operator
            .stage_checkpointed_shuffle_frontier(
                "temporal::left",
                2,
                InputFrontier {
                    watermark: Some(250),
                    idle: false,
                },
                assignment,
                recovery,
            )
            .unwrap();
        operator.remote_peer_cursors = [Some(2); 2];
        let capture = operator
            .checkpoint_capture(u64::MAX)
            .unwrap()
            .expect("queued cluster state must capture");
        let local = operator.local_frontiers;
        operator.process_with_frontiers(&[], &local).await.unwrap();
        assert_eq!(operator.queued_remote_events, 0);
        let data = materialize_capture(capture).to_vec();
        (OperatorCheckpoint { data }, scope)
    }

    #[cfg(feature = "cluster")]
    fn assert_cluster_restore_pristine(operator: &ManagedTemporalJoinOperator) {
        assert_eq!(operator.whole_restore, WholeRestoreState::Pending);
        assert_eq!(operator.frontiers, [InputFrontier::default(); 2]);
        assert_eq!(operator.local_frontiers, [InputFrontier::default(); 2]);
        assert_eq!(operator.last_broadcasts, [InputFrontier::default(); 2]);
        assert_eq!(operator.remote_peer_cursors, [None; 2]);
        assert!(operator.published_output_frontier.is_none());
        assert_eq!(operator.queued_shuffle_bytes, 0);
        assert_eq!(operator.queued_remote_events, 0);
        assert_eq!(operator.queued_event_capacity_bytes, 0);
        assert!(operator.peer_channels.iter().flatten().all(|(_, channel)| {
            channel.applied == InputFrontier::default()
                && channel.accepted == InputFrontier::default()
                && channel.events.is_empty()
        }));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_channel_checkpoint_round_trip_preserves_order_and_replay() {
        let (checkpoint, scope) = queued_cluster_checkpoint().await;
        let checkpoint_data = checkpoint.data.clone();
        let (mut tight, _, _) = operator(8);
        tight.attach_cluster_shuffle(scope.clone());
        let preflight = tight
            .preflight_whole_checkpoint_archive(&checkpoint_data, "tight queued checkpoint", |_| {
                Ok(())
            })
            .unwrap();
        assert!(preflight.runtime_scratch > 0);
        let live_bytes = tight.checked_accounted_state_bytes().unwrap();
        let runtime_peak = live_bytes
            .checked_add(preflight.decoded_checkpoint)
            .and_then(|bytes| bytes.checked_add(preflight.runtime_scratch))
            .unwrap();
        tight.set_managed_state_budget(runtime_peak - 1);
        assert!(matches!(
            tight.restore(OperatorCheckpoint {
                data: checkpoint_data.clone(),
            }),
            Err(DbError::ManagedStateBudgetExceeded { .. })
        ));
        assert_cluster_restore_pristine(&tight);

        scope.sender.set_recovery_gen(1);
        scope.receiver.set_recovery_gen(1);
        let (mut restored, _, _) = operator(8);
        restored.attach_cluster_shuffle(scope.clone());
        restored.restore(checkpoint).unwrap();

        assert_eq!(restored.queued_remote_events, 6);
        assert_eq!(restored.remote_peer_cursors, [Some(2); 2]);
        for port in 0..2 {
            let channel = &restored.peer_channels[port][&2];
            assert_eq!(channel.applied.watermark, Some(100));
            assert_eq!(channel.accepted.watermark, Some(250));
        }
        assert!(matches!(
            &restored.peer_channels[0][&2].events[0].payload,
            TemporalRemoteEventPayload::Data(_)
        ));
        assert!(matches!(
            &restored.peer_channels[0][&2].events[1].payload,
            TemporalRemoteEventPayload::Frontier(_)
        ));
        assert!(matches!(
            &restored.peer_channels[1][&2].events[0].payload,
            TemporalRemoteEventPayload::Data(_)
        ));
        assert!(matches!(
            &restored.peer_channels[1][&2].events[1].payload,
            TemporalRemoteEventPayload::Data(_)
        ));
        assert!(matches!(
            &restored.peer_channels[1][&2].events[2].payload,
            TemporalRemoteEventPayload::Data(_)
        ));
        assert!(matches!(
            &restored.peer_channels[1][&2].events[3].payload,
            TemporalRemoteEventPayload::Frontier(_)
        ));
        let right_streams = restored.peer_channels[1][&2]
            .events
            .iter()
            .filter_map(|event| match &event.payload {
                TemporalRemoteEventPayload::Data(batch) => Some(batch.mutation_stream),
                TemporalRemoteEventPayload::Frontier(_) => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(right_streams, [false, true, false]);
        assert_eq!(
            restored.restored_output_frontier(),
            Some(InputFrontier {
                watermark: Some(100),
                idle: false,
            })
        );
        assert!(restored.checkpoint().unwrap().is_some());

        let local = restored.local_frontiers;
        let output = restored.process_with_frontiers(&[], &local).await.unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(restored.queued_remote_events, 0);
        assert!(!restored.has_remote_events());
        let value = output[0]
            .column_by_name("value_quotes")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(value.value(0), "live");

        let idle = InputFrontier {
            watermark: Some(100),
            idle: true,
        };
        let channel = restored.peer_channels[1].get_mut(&2).unwrap();
        channel.applied = idle;
        channel.accepted = idle;
        restored.pending_frontiers = Some([
            restored.frontiers[0],
            InputFrontier {
                watermark: Some(500),
                idle: false,
            },
        ]);
        let assignment = scope.registry.assignment_version();
        let recovery = scope.receiver.recovery_gen();
        restored
            .stage_checkpointed_shuffle_frontier(
                "temporal::right",
                2,
                InputFrontier {
                    watermark: Some(100),
                    idle: false,
                },
                assignment,
                recovery,
            )
            .unwrap();
        assert_eq!(restored.peer_channels[1][&2].accepted.watermark, Some(500));
        let key = key_for_vnode(0);
        let late = crate::operator::RetainedBatch::restored_channel(
            right_batch(
                std::slice::from_ref(&key),
                &["X"],
                &[499],
                &["late"],
                &[SourceMutation::Put],
            ),
            2,
            assignment,
            recovery,
            Arc::from([0_u32]),
        );
        let error = restored
            .stage_checkpointed_shuffle("temporal::right", late, i64::MIN)
            .unwrap_err();
        assert!(error.to_string().contains("applied frontier 500"));
        restored
            .stage_checkpointed_shuffle_frontier(
                "temporal::right",
                2,
                InputFrontier {
                    watermark: Some(150),
                    idle: false,
                },
                assignment,
                recovery,
            )
            .unwrap();
        assert_eq!(restored.peer_channels[1][&2].accepted.watermark, Some(500));
        restored
            .stage_checkpointed_shuffle_frontier(
                "temporal::right",
                2,
                InputFrontier {
                    watermark: Some(550),
                    idle: false,
                },
                assignment,
                recovery,
            )
            .unwrap();
        assert_eq!(restored.peer_channels[1][&2].accepted.watermark, Some(550));
        assert!(restored
            .stage_checkpointed_shuffle_frontier(
                "temporal::right",
                2,
                InputFrontier {
                    watermark: None,
                    idle: false,
                },
                assignment,
                recovery,
            )
            .is_err());
        assert!(restored
            .stage_checkpointed_shuffle_frontier(
                "temporal::right",
                2,
                InputFrontier {
                    watermark: Some(525),
                    idle: false,
                },
                assignment,
                recovery,
            )
            .is_err());
        assert_eq!(restored.peer_channels[1][&2].accepted.watermark, Some(550));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_checkpoint_restore_rejects_topology_and_routes_atomically() {
        let (checkpoint, scope) = queued_cluster_checkpoint().await;
        let mut wrong_rows =
            rkyv::from_bytes::<TemporalJoinOperatorCheckpoint, rkyv::rancor::Error>(
                &checkpoint.data,
            )
            .unwrap();
        let wrong_row_count = wrong_rows
            .cluster
            .as_mut()
            .unwrap()
            .channels
            .iter_mut()
            .flatten()
            .flat_map(|channel| channel.events.iter_mut())
            .find_map(|event| match event {
                TemporalCheckpointEvent::Data { row_count, .. } => Some(row_count),
                TemporalCheckpointEvent::Frontier { .. } => None,
            })
            .unwrap();
        *wrong_row_count = wrong_row_count.checked_add(1).unwrap();
        let wrong_rows = OperatorCheckpoint {
            data: rkyv::to_bytes::<rkyv::rancor::Error>(&wrong_rows)
                .unwrap()
                .to_vec(),
        };
        let (mut restored, _, _) = operator(8);
        restored.attach_cluster_shuffle(scope.clone());
        assert!(restored.restore(wrong_rows).is_err());
        assert_cluster_restore_pristine(&restored);

        let mut wrong_topology = rkyv::from_bytes::<
            TemporalJoinOperatorCheckpoint,
            rkyv::rancor::Error,
        >(&checkpoint.data)
        .unwrap();
        wrong_topology.cluster.as_mut().unwrap().owner_map_digest[0] ^= 0xff;
        let wrong_topology = OperatorCheckpoint {
            data: rkyv::to_bytes::<rkyv::rancor::Error>(&wrong_topology)
                .unwrap()
                .to_vec(),
        };
        let (mut restored, _, _) = operator(8);
        restored.attach_cluster_shuffle(scope.clone());
        assert!(restored.restore(wrong_topology).is_err());
        assert_cluster_restore_pristine(&restored);

        let mut wrong_route =
            rkyv::from_bytes::<TemporalJoinOperatorCheckpoint, rkyv::rancor::Error>(
                &checkpoint.data,
            )
            .unwrap();
        let mut changed = false;
        for channel in &mut wrong_route.cluster.as_mut().unwrap().channels[1] {
            for event in &mut channel.events {
                if let TemporalCheckpointEvent::Data { routed_vnodes, .. } = event {
                    *routed_vnodes = vec![1];
                    changed = true;
                    break;
                }
            }
            if changed {
                break;
            }
        }
        assert!(changed);
        let wrong_route = OperatorCheckpoint {
            data: rkyv::to_bytes::<rkyv::rancor::Error>(&wrong_route)
                .unwrap()
                .to_vec(),
        };
        let (mut restored, _, _) = operator(8);
        restored.attach_cluster_shuffle(scope);
        assert!(restored.restore(wrong_route).is_err());
        assert_cluster_restore_pristine(&restored);
    }
}
