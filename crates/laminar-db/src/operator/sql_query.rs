//! Standard SQL query operator with lazy initialization.
//!
//! Handles all non-EOWC, non-join queries. On first `process()` call,
//! introspects the SQL via `DataFusion` to determine the execution path:
//! - Aggregate (GROUP BY) -> incremental accumulators
//! - Simple single-source -> compiled `PhysicalExpr` projection
//! - Complex non-aggregate -> cached physical plan (`LiveSourceExec` reads fresh data)

use std::ops::ControlFlow;
use std::sync::Arc;

#[cfg(feature = "cluster")]
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use async_trait::async_trait;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionContext;
#[cfg(feature = "cluster")]
use laminar_core::shuffle::ShuffleMessage;
use laminar_core::state::KeyGroupCount;
#[cfg(feature = "cluster")]
use laminar_core::state::{NodeId, VnodeAssignmentSnapshot};
use sqlparser::ast::{
    visit_expressions, Expr, GroupByExpr, Query, Select, SetExpr, Statement, TableFactor,
};

use crate::aggregate_state::{
    apply_compiled_having, AggStateCheckpoint, CompiledProjection, IncrementalAggState,
};
#[cfg(feature = "cluster")]
use crate::aggregate_state::{
    AggStateRestorePreflight, OwnedAggVnodeRestore, PreparedAggVnodeTransition,
    RetiredAggVnodeTransition,
};
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
#[cfg(feature = "cluster")]
use crate::operator::capability::{ManagedStateContract, OperatorStateClass};
use crate::operator::capability::{OperatorCapability, OperatorImplementation};
#[cfg(feature = "cluster")]
use crate::operator_graph::{
    merge_input_frontier_iter, ManagedVnodeTransition, ManagedVnodeTransitionMode,
};
use crate::operator_graph::{
    try_evaluate_compiled, CapturedVnodeState, EncodedStateFrame, GraphOperator, InputFrontier,
    ManagedStateAccountingSnapshot, OperatorCheckpoint, StateFrameCapture,
};
use crate::sql_analysis::{
    extract_projection_filter, projection_sql_preserving_weight, single_source_table,
};

// Keep batches created by local aggregate coalescing small enough to bound both accumulator work
// and the temporary old-plus-new Arrow buffers held while concatenating one group.
const LOCAL_AGG_COALESCE_TARGET_BATCH_BYTES: usize = 256 * 1024;
const LOCAL_AGG_COALESCE_MAX_BATCH_ROWS: usize = 1_024;

/// Whether Arrow concatenation is representation-stable for this local aggregate input type.
///
/// Dictionary and nested encodings can make independently valid batches fail only when their
/// value spaces or offsets are merged. Preserve their original apply boundaries unless a
/// type-specific coalescing proof is added.
fn certifies_local_aggregate_concat_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// Coalesce already-projected local aggregate input without changing row or schema order.
///
/// The input is consumed so concatenation overlaps new Arrow buffers with only the current group.
/// Existing oversized batches are preserved: these limits constrain only batches created here.
fn coalesce_local_aggregate_batches(
    op_name: &str,
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, DbError> {
    // Retraction validity is checked at each weighted batch boundary. For example, [-1] then
    // [+1] must reject the invalid prefix rather than being merged into a valid-looking zero.
    if batches.iter().any(|batch| {
        batch
            .schema()
            .index_of(laminar_core::changelog::WEIGHT_COLUMN)
            .is_ok()
    }) {
        return Ok(batches);
    }

    if batches.iter().any(|batch| {
        batch
            .schema()
            .fields()
            .iter()
            .any(|field| !certifies_local_aggregate_concat_type(field.data_type()))
    }) {
        return Ok(batches);
    }

    fn flush_group(
        op_name: &str,
        group: &mut Vec<RecordBatch>,
        output: &mut Vec<RecordBatch>,
    ) -> Result<(), DbError> {
        match group.len() {
            0 => Ok(()),
            1 => {
                output.push(
                    group
                        .pop()
                        .expect("single aggregate coalescing group batch"),
                );
                Ok(())
            }
            _ => {
                let schema = group[0].schema();
                let combined = arrow::compute::concat_batches(&schema, group.as_slice())
                    .map_err(|error| {
                        DbError::Pipeline(format!(
                            "aggregate '{op_name}' local input concat failed before state application: {error}"
                        ))
                    })?;
                let logical_bytes = laminar_core::shuffle::logical_batch_bytes(&combined)
                    .map_err(|error| {
                        DbError::Pipeline(format!(
                            "aggregate '{op_name}' local input size accounting failed before state application: {error}"
                        ))
                    })?;
                if combined.num_rows() > LOCAL_AGG_COALESCE_MAX_BATCH_ROWS
                    || logical_bytes > LOCAL_AGG_COALESCE_TARGET_BATCH_BYTES
                {
                    return Err(DbError::Pipeline(format!(
                        "aggregate '{op_name}' coalesced local input exceeded its target before state application: {} rows/{logical_bytes} bytes (limits: {} rows/{} bytes)",
                        combined.num_rows(),
                        LOCAL_AGG_COALESCE_MAX_BATCH_ROWS,
                        LOCAL_AGG_COALESCE_TARGET_BATCH_BYTES,
                    )));
                }
                group.clear();
                output.push(combined);
                Ok(())
            }
        }
    }

    let mut output = Vec::new();
    let mut group = Vec::new();
    let mut group_rows = 0usize;
    let mut group_bytes = 0usize;

    for batch in batches.into_iter().filter(|batch| batch.num_rows() != 0) {
        let batch_rows = batch.num_rows();
        let batch_bytes =
            laminar_core::shuffle::logical_batch_bytes(&batch).map_err(|error| {
                DbError::Pipeline(format!(
                    "aggregate '{op_name}' local input size accounting failed before state application: {error}"
                ))
            })?;
        let independently_coalescible = batch.num_rows() <= LOCAL_AGG_COALESCE_MAX_BATCH_ROWS
            && batch_bytes <= LOCAL_AGG_COALESCE_TARGET_BATCH_BYTES;
        if !independently_coalescible {
            flush_group(op_name, &mut group, &mut output)?;
            group_rows = 0;
            group_bytes = 0;
            output.push(batch);
            continue;
        }

        let same_schema = group
            .first()
            .is_none_or(|first: &RecordBatch| first.schema().as_ref() == batch.schema().as_ref());
        let next_rows = group_rows.checked_add(batch.num_rows());
        let next_bytes = group_bytes.checked_add(batch_bytes);
        let fits = same_schema
            && next_rows.is_some_and(|rows| rows <= LOCAL_AGG_COALESCE_MAX_BATCH_ROWS)
            && next_bytes.is_some_and(|bytes| bytes <= LOCAL_AGG_COALESCE_TARGET_BATCH_BYTES);
        if !fits {
            flush_group(op_name, &mut group, &mut output)?;
            group_rows = 0;
            group_bytes = 0;
        }
        group.push(batch);
        group_rows += batch_rows;
        group_bytes += batch_bytes;
    }
    flush_group(op_name, &mut group, &mut output)?;
    Ok(output)
}

// Resolved on first `process()` call by introspecting the SQL.
enum QueryState {
    Uninit,
    Agg(Box<IncrementalAggState>),
    Compiled(CompiledProjection),
    // Single-source, non-compilable; `LiveSourceExec` feeds fresh data per cycle.
    CachedPlan(Arc<dyn datafusion::physical_plan::ExecutionPlan>),
    // Multi-source JOIN; the hash table is rebuilt each cycle from fresh live-source data.
    CachedPhysical(Arc<dyn datafusion::physical_plan::ExecutionPlan>),
}

/// A shallowly retained, final local full-state aggregate emission.
///
/// `retained_bytes` conservatively charges the Arrow backing allocations and batch/column rosters
/// held by `batches`. Returned batches are shallow clones of these `RecordBatch` handles and do not
/// add another cache charge.
struct CachedLocalAggregateOutput {
    batches: Vec<RecordBatch>,
    retained_bytes: usize,
}

#[cfg(feature = "cluster")]
struct PreparedSqlVnodeTransition {
    aggregate: PreparedAggVnodeTransition,
    topology: PreparedAggTopology,
}

#[cfg(feature = "cluster")]
enum SqlVnodeTransitionCleanup {
    Aborted(PreparedSqlVnodeTransition),
    Published {
        aggregate: RetiredAggVnodeTransition,
        topology: PreparedAggTopology,
    },
}

#[cfg(feature = "cluster")]
struct PreparedAggTopology {
    assignment: VnodeAssignmentSnapshot,
    assignment_digest: [u8; 32],
    peers: Arc<[u64]>,
    channels: BTreeMap<u64, AggPeerChannel>,
    local_frontier: InputFrontier,
    last_broadcast: InputFrontier,
    effective_frontier: InputFrontier,
}

/// Pre-aggregate row-shuffle config for cluster mode.
#[cfg(feature = "cluster")]
#[derive(Clone)]
pub struct ClusterShuffleConfig {
    pub registry: Arc<laminar_core::state::VnodeRegistry>,
    pub sender: Arc<laminar_core::shuffle::ShuffleSender>,
    pub receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    pub self_id: laminar_core::state::NodeId,
}

#[cfg(feature = "cluster")]
impl std::fmt::Debug for ClusterShuffleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterShuffleConfig")
            .field("self_id", &self.self_id)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "cluster")]
const AGG_OP_CHECKPOINT_VERSION: u8 = 2;
const AGG_CHECKPOINT_ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;

fn aggregate_checkpoint_alignment_copy_bytes(bytes: &[u8]) -> usize {
    if bytes
        .as_ptr()
        .align_offset(AGG_CHECKPOINT_ARCHIVE_ALIGNMENT)
        == 0
    {
        0
    } else {
        bytes.len()
    }
}

fn with_aligned_aggregate_checkpoint_bytes<T>(
    bytes: &[u8],
    decode: impl FnOnce(&[u8]) -> Result<T, DbError>,
) -> Result<T, DbError> {
    let aligned;
    let bytes = if aggregate_checkpoint_alignment_copy_bytes(bytes) == 0 {
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
fn aggregate_transition_roster_scratch_bytes(
    donor_capacity: usize,
    preflight_capacity: usize,
    lower_bound_capacity: usize,
) -> Option<usize> {
    donor_capacity
        .checked_mul(std::mem::size_of::<u64>())
        .and_then(|bytes| {
            preflight_capacity
                .checked_mul(std::mem::size_of::<(u32, &[u8], AggStateRestorePreflight)>())
                .and_then(|preflight| bytes.checked_add(preflight))
        })
        .and_then(|bytes| {
            lower_bound_capacity
                .checked_mul(std::mem::size_of::<(u32, usize)>())
                .and_then(|lower_bounds| bytes.checked_add(lower_bounds))
        })
}

#[cfg(feature = "cluster")]
#[derive(Clone, Copy, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct AggCheckpointFrontier {
    watermark: Option<i64>,
    idle: bool,
}

#[cfg(feature = "cluster")]
impl From<InputFrontier> for AggCheckpointFrontier {
    fn from(frontier: InputFrontier) -> Self {
        Self {
            watermark: frontier.watermark,
            idle: frontier.idle,
        }
    }
}

#[cfg(feature = "cluster")]
impl From<AggCheckpointFrontier> for InputFrontier {
    fn from(frontier: AggCheckpointFrontier) -> Self {
        Self {
            watermark: frontier.watermark,
            idle: frontier.idle,
        }
    }
}

#[cfg(feature = "cluster")]
fn archived_agg_checkpoint_frontier(frontier: &ArchivedAggCheckpointFrontier) -> InputFrontier {
    InputFrontier {
        watermark: frontier
            .watermark
            .as_ref()
            .map(|watermark| watermark.to_native()),
        idle: frontier.idle,
    }
}

#[cfg(feature = "cluster")]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
enum AggCheckpointEvent {
    Data {
        recovery_gen: u64,
        routed_vnodes: Vec<u32>,
        ipc: Vec<u8>,
    },
    Frontier {
        recovery_gen: u64,
        frontier: AggCheckpointFrontier,
    },
}

#[cfg(feature = "cluster")]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct AggCheckpointChannel {
    peer: u64,
    applied: AggCheckpointFrontier,
    events: Vec<AggCheckpointEvent>,
}

#[cfg(feature = "cluster")]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct AggOpCheckpoint {
    version: u8,
    assignment_version: u64,
    owner_map_digest: [u8; 32],
    self_id: u64,
    recovery_gen: u64,
    local_frontier: AggCheckpointFrontier,
    effective_frontier: AggCheckpointFrontier,
    remote_peer_cursor: Option<u64>,
    channels: Vec<AggCheckpointChannel>,
}

#[cfg(feature = "cluster")]
#[derive(Clone)]
struct AggQueuedBatch {
    retained: Arc<crate::operator::RetainedBatch>,
    row_vnodes: Arc<[u32]>,
    charged_bytes: usize,
}

#[cfg(feature = "cluster")]
#[derive(Clone)]
enum AggRemoteEventPayload {
    Data(AggQueuedBatch),
    Frontier(InputFrontier),
}

#[cfg(feature = "cluster")]
#[derive(Clone)]
struct AggRemoteEvent {
    assignment_version: u64,
    recovery_gen: u64,
    payload: AggRemoteEventPayload,
}

#[cfg(feature = "cluster")]
impl AggRemoteEvent {
    fn payload_bytes(&self) -> usize {
        match &self.payload {
            AggRemoteEventPayload::Data(batch) => batch.charged_bytes,
            AggRemoteEventPayload::Frontier(_) => 0,
        }
    }
}

#[cfg(feature = "cluster")]
#[derive(Default)]
struct AggPeerChannel {
    applied: InputFrontier,
    accepted: InputFrontier,
    events: VecDeque<AggRemoteEvent>,
}

#[cfg(feature = "cluster")]
struct AggClusterInputPlan {
    local_batches: Vec<(RecordBatch, Option<u32>)>,
    outbound: Vec<(u64, ShuffleMessage)>,
    local_frontier: InputFrontier,
    effective_frontier: InputFrontier,
}

#[cfg(feature = "cluster")]
type AggSendOutcome = (Result<(), DbError>, Option<Vec<(u64, ShuffleMessage)>>);

#[cfg(feature = "cluster")]
type AggSendTask = tokio::task::JoinHandle<()>;

#[cfg(feature = "cluster")]
struct PendingAggClusterInput {
    local_batches: Vec<(RecordBatch, Option<u32>)>,
    outbound: Option<Vec<(u64, ShuffleMessage)>>,
    local_frontier: InputFrontier,
    send: Option<AggSendTask>,
    completion: Option<tokio::sync::oneshot::Receiver<AggSendOutcome>>,
    accounted_bytes: usize,
}

#[cfg(feature = "cluster")]
impl Drop for PendingAggClusterInput {
    fn drop(&mut self) {
        if let Some(send) = &self.send {
            send.abort();
        }
    }
}

#[cfg(feature = "cluster")]
enum PendingAggCompletion {
    Waiting,
    RetryLater,
    Applied(Vec<RecordBatch>),
}

#[cfg(feature = "cluster")]
enum CapturedAggEvent {
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
struct CapturedAggChannel {
    peer: u64,
    applied: InputFrontier,
    events: Vec<CapturedAggEvent>,
}

#[cfg(feature = "cluster")]
struct AggCheckpointCapture {
    assignment_version: u64,
    owner_map_digest: [u8; 32],
    self_id: u64,
    recovery_gen: u64,
    local_frontier: InputFrontier,
    effective_frontier: InputFrontier,
    remote_peer_cursor: Option<u64>,
    channels: Vec<CapturedAggChannel>,
    retained_bytes: u64,
}

#[cfg(feature = "cluster")]
const AGG_REMOTE_EVENT_CHARGE: usize = std::mem::size_of::<AggRemoteEvent>();
#[cfg(feature = "cluster")]
const AGG_RETAINED_BATCH_ARC_CHARGE: usize =
    std::mem::size_of::<crate::operator::RetainedBatch>() + 2 * std::mem::size_of::<usize>();
#[cfg(feature = "cluster")]
const AGG_ROW_VNODE_ARC_CHARGE: usize = 2 * std::mem::size_of::<usize>();
#[cfg(feature = "cluster")]
const AGG_PEER_CHANNEL_ENTRY_CHARGE: usize = 64;

#[cfg(feature = "cluster")]
impl PreparedAggTopology {
    fn accounted_state_bytes(&self) -> usize {
        self.assignment
            .owners()
            .len()
            .saturating_mul(std::mem::size_of::<NodeId>() + std::mem::size_of::<u64>())
            .saturating_add(self.peers.len().saturating_mul(std::mem::size_of::<u64>()))
            .saturating_add(self.channels.len().saturating_mul(
                std::mem::size_of::<(u64, AggPeerChannel)>() + AGG_PEER_CHANNEL_ENTRY_CHARGE,
            ))
            .saturating_add(self.channels.values().fold(0usize, |total, channel| {
                total.saturating_add(
                    channel
                        .events
                        .capacity()
                        .saturating_mul(AGG_REMOTE_EVENT_CHARGE),
                )
            }))
    }
}

fn serialize_agg_cp(
    cp: &AggStateCheckpoint,
    op_name: &str,
    max_encoded_bytes: usize,
) -> Result<EncodedStateFrame, DbError> {
    let writer = rkyv::ser::writer::IoWriter::new(
        laminar_core::serialization::BoundedBytesWriter::new(max_encoded_bytes),
    );
    rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(cp, writer)
        .map(|bytes| EncodedStateFrame::from_vec(bytes.into_inner().into_vec()))
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "aggregate '{op_name}' vnode checkpoint exceeded its {max_encoded_bytes}-byte limit: {error}"
            ))
        })
}

#[cfg(feature = "cluster")]
fn encode_agg_checkpoint_capture(
    op_name: &str,
    capture: AggCheckpointCapture,
    max_working_bytes: usize,
) -> Result<EncodedStateFrame, DbError> {
    let mut working_bytes = 0usize;
    let mut channels = Vec::new();
    channels
        .try_reserve_exact(capture.channels.len())
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "aggregate '{op_name}' channel checkpoint could not reserve metadata: {error}"
            ))
        })?;
    working_bytes = working_bytes
        .checked_add(
            channels
                .capacity()
                .checked_mul(std::mem::size_of::<AggCheckpointChannel>())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "aggregate '{op_name}' channel checkpoint metadata overflow"
                    ))
                })?,
        )
        .filter(|bytes| *bytes <= max_working_bytes)
        .ok_or_else(|| {
            DbError::Checkpoint(format!(
                "aggregate '{op_name}' channel checkpoint metadata exceeds its {max_working_bytes}-byte working limit"
            ))
        })?;

    for captured in capture.channels {
        let mut events = Vec::new();
        events
            .try_reserve_exact(captured.events.len())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "aggregate '{op_name}' channel checkpoint could not reserve events: {error}"
                ))
            })?;
        working_bytes = working_bytes
            .checked_add(
                events
                    .capacity()
                    .checked_mul(std::mem::size_of::<AggCheckpointEvent>())
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "aggregate '{op_name}' channel event metadata overflow"
                        ))
                    })?,
            )
            .filter(|bytes| *bytes <= max_working_bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aggregate '{op_name}' channel event metadata exceeds its working limit"
                ))
            })?;
        for event in captured.events {
            match event {
                CapturedAggEvent::Data {
                    recovery_gen,
                    retained,
                } => {
                    let routed_vnodes = retained.routed_vnodes().to_vec();
                    working_bytes = working_bytes
                        .checked_add(
                            routed_vnodes
                                .capacity()
                                .checked_mul(std::mem::size_of::<u32>())
                                .ok_or_else(|| {
                                    DbError::Checkpoint(format!(
                                        "aggregate '{op_name}' channel route metadata overflow"
                                    ))
                                })?,
                        )
                        .filter(|bytes| *bytes <= max_working_bytes)
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "aggregate '{op_name}' channel route metadata exceeds its working limit"
                            ))
                        })?;
                    let remaining =
                        max_working_bytes
                            .checked_sub(working_bytes)
                            .ok_or_else(|| {
                                DbError::Checkpoint(format!(
                            "aggregate '{op_name}' channel checkpoint exhausted its working limit"
                        ))
                            })?;
                    let ipc = laminar_core::serialization::serialize_batches_stream_bounded(
                        retained.batch().schema().as_ref(),
                        std::iter::once(retained.batch()),
                        remaining,
                    )
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "aggregate '{op_name}' channel IPC serialization: {error}"
                        ))
                    })?;
                    working_bytes = working_bytes
                        .checked_add(ipc.capacity())
                        .filter(|bytes| *bytes <= max_working_bytes)
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "aggregate '{op_name}' channel IPC exceeds its working limit"
                            ))
                        })?;
                    events.push(AggCheckpointEvent::Data {
                        recovery_gen,
                        routed_vnodes,
                        ipc,
                    });
                }
                CapturedAggEvent::Frontier {
                    recovery_gen,
                    frontier,
                } => events.push(AggCheckpointEvent::Frontier {
                    recovery_gen,
                    frontier: frontier.into(),
                }),
            }
        }
        channels.push(AggCheckpointChannel {
            peer: captured.peer,
            applied: captured.applied.into(),
            events,
        });
    }

    let archive_budget = max_working_bytes
        .checked_sub(working_bytes)
        .ok_or_else(|| {
            DbError::Checkpoint(format!(
                "aggregate '{op_name}' channel checkpoint exhausted its working limit before archive serialization"
            ))
        })?;
    let writer = rkyv::ser::writer::IoWriter::new(
        laminar_core::serialization::BoundedBytesWriter::new(archive_budget),
    );
    rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(
        &AggOpCheckpoint {
            version: AGG_OP_CHECKPOINT_VERSION,
            assignment_version: capture.assignment_version,
            owner_map_digest: capture.owner_map_digest,
            self_id: capture.self_id,
            recovery_gen: capture.recovery_gen,
            local_frontier: capture.local_frontier.into(),
            effective_frontier: capture.effective_frontier.into(),
            remote_peer_cursor: capture.remote_peer_cursor,
            channels,
        },
        writer,
    )
    .map(|bytes| EncodedStateFrame::from_vec(bytes.into_inner().into_vec()))
    .map_err(|error| {
        DbError::Checkpoint(format!(
            "aggregate '{op_name}' channel archive serialization exceeded its {archive_budget}-byte headroom: {error}"
        ))
    })
}

fn is_direct_single_source_shape(query: &Query, select: &Select) -> bool {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
        || select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || select.from.len() != 1
        || !select.from[0].joins.is_empty()
    {
        return false;
    }

    matches!(
        &select.from[0].relation,
        TableFactor::Table {
            args: None,
            with_hints,
            version: None,
            with_ordinality: false,
            partitions,
            json_path: None,
            sample: None,
            index_hints,
            ..
        } if with_hints.is_empty() && partitions.is_empty() && index_hints.is_empty()
    )
}

fn is_stream_window_marker(name: &str) -> bool {
    matches!(name, "TUMBLE" | "HOP" | "SLIDE" | "SESSION" | "CUMULATE")
}

/// Conservatively classify the immutable SQL before lazy execution-path initialization.
///
/// The parser analysis is exact for direct aggregate and single-source projection/filter shapes.
/// Complex structure, unknown functions, analytics, and unrecognized grouping remain local-only;
/// the descriptor must never guess "stateless".
fn classify_sql_capability(sql: &str, ctx: &SessionContext) -> OperatorCapability {
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return OperatorCapability::unclassified_sql_query();
    };
    if statements.len() != 1 {
        return OperatorCapability::unclassified_sql_query();
    }
    let Some(laminar_sql::parser::StreamingStatement::Standard(statement)) = statements.first()
    else {
        return OperatorCapability::unclassified_sql_query();
    };
    let Statement::Query(query) = statement.as_ref() else {
        return OperatorCapability::unclassified_sql_query();
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return OperatorCapability::unclassified_sql_query();
    };

    let function_registry = ctx.state();
    let mut has_registered_aggregate = false;
    let mut has_stream_window = false;
    let mut has_ambiguous_expression = false;
    let _ = visit_expressions(statement.as_ref(), |expression| {
        if let Expr::Function(function) = expression {
            if function.over.is_some() {
                has_ambiguous_expression = true;
                return ControlFlow::Break(());
            }
            let name = function.name.to_string();
            let normalized = name.to_ascii_lowercase();
            if function_registry
                .aggregate_functions()
                .contains_key(&normalized)
            {
                has_registered_aggregate = true;
            } else if is_stream_window_marker(&name.to_ascii_uppercase()) {
                has_stream_window = true;
            } else if !function_registry
                .scalar_functions()
                .contains_key(&normalized)
            {
                has_ambiguous_expression = true;
                return ControlFlow::Break(());
            }
        } else if matches!(
            expression,
            Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::Subquery(_)
        ) {
            has_ambiguous_expression = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    if has_ambiguous_expression {
        return OperatorCapability::unclassified_sql_query();
    }

    let aggregation =
        laminar_sql::parser::aggregation_parser::analyze_aggregates(statement.as_ref());
    let has_grouping = match &select.group_by {
        GroupByExpr::Expressions(expressions, _) => !expressions.is_empty(),
        GroupByExpr::All(_) => true,
    };
    if !is_direct_single_source_shape(query, select) {
        return OperatorCapability::unclassified_sql_query();
    }

    if aggregation.has_aggregates() || has_registered_aggregate {
        return if has_grouping {
            if has_stream_window {
                OperatorCapability::windowed_keyed_sql_aggregate()
            } else {
                OperatorCapability::keyed_sql_aggregate()
            }
        } else {
            OperatorCapability::global_sql_aggregate()
        };
    }

    if has_grouping || select.having.is_some() {
        OperatorCapability::unclassified_sql_query()
    } else {
        OperatorCapability::stateless_sql_query()
    }
}

pub(crate) struct SqlQueryOperator {
    op_name: Arc<str>,
    sql: String,
    capability: OperatorCapability,
    ctx: SessionContext,
    task_ctx: Arc<TaskContext>,
    state: QueryState,
    key_group_count: KeyGroupCount,
    prom: Option<Arc<EngineMetrics>>,
    execution_path_logged: bool,
    emit_changelog: bool,
    max_managed_state_bytes: usize,
    cached_local_aggregate_output: Option<CachedLocalAggregateOutput>,
    #[cfg(feature = "cluster")]
    cluster_shuffle: Option<ClusterShuffleConfig>,
    #[cfg(feature = "cluster")]
    cluster_assignment: Option<VnodeAssignmentSnapshot>,
    #[cfg(feature = "cluster")]
    cluster_assignment_digest: Option<[u8; 32]>,
    #[cfg(feature = "cluster")]
    cluster_peers: Arc<[u64]>,
    #[cfg(feature = "cluster")]
    peer_channels: BTreeMap<u64, AggPeerChannel>,
    #[cfg(feature = "cluster")]
    local_frontier: InputFrontier,
    #[cfg(feature = "cluster")]
    last_broadcast: InputFrontier,
    #[cfg(feature = "cluster")]
    effective_frontier: InputFrontier,
    #[cfg(feature = "cluster")]
    remote_peer_cursor: Option<u64>,
    #[cfg(feature = "cluster")]
    queued_payload_bytes: usize,
    #[cfg(feature = "cluster")]
    queued_event_capacity_bytes: usize,
    #[cfg(feature = "cluster")]
    queued_remote_events: usize,
    #[cfg(feature = "cluster")]
    pending_cluster_input: Option<PendingAggClusterInput>,
    #[cfg(feature = "cluster")]
    whole_restore_applied: bool,
    #[cfg(feature = "cluster")]
    prepared_vnode_transition: Option<PreparedSqlVnodeTransition>,
    #[cfg(feature = "cluster")]
    vnode_transition_cleanup: Option<SqlVnodeTransitionCleanup>,
}

/// Classify a failure from a step that may already have changed operator state.
///
/// Ordinary errors are not safe to isolate or retry once mutation may have begun. Existing
/// recovery and halt dispositions retain their stronger classification.
fn stateful_apply_outcome_unknown(op_name: &str, phase: &str, error: DbError) -> DbError {
    if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
        return error;
    }
    DbError::StatefulOperatorPartialApply(format!(
        "aggregate '{op_name}' {phase} failed after state application began; the apply outcome is unknown: {error}"
    ))
}

impl SqlQueryOperator {
    #[cfg(test)]
    pub fn new(
        name: &str,
        sql: &str,
        ctx: SessionContext,
        prom: Option<Arc<EngineMetrics>>,
        emit_changelog: bool,
    ) -> Self {
        Self::new_with_key_groups(
            name,
            sql,
            ctx,
            prom,
            emit_changelog,
            KeyGroupCount::try_from(1_u16).expect("one test key group is valid"),
        )
    }

    pub fn new_with_key_groups(
        name: &str,
        sql: &str,
        ctx: SessionContext,
        prom: Option<Arc<EngineMetrics>>,
        emit_changelog: bool,
        key_group_count: KeyGroupCount,
    ) -> Self {
        let capability = classify_sql_capability(sql, &ctx);
        let task_ctx = ctx.task_ctx();
        Self {
            op_name: Arc::from(name),
            sql: sql.to_string(),
            capability,
            ctx,
            task_ctx,
            state: QueryState::Uninit,
            key_group_count,
            prom,
            execution_path_logged: false,
            emit_changelog,
            max_managed_state_bytes: crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            cached_local_aggregate_output: None,
            #[cfg(feature = "cluster")]
            cluster_shuffle: None,
            #[cfg(feature = "cluster")]
            cluster_assignment: None,
            #[cfg(feature = "cluster")]
            cluster_assignment_digest: None,
            #[cfg(feature = "cluster")]
            cluster_peers: Arc::from([]),
            #[cfg(feature = "cluster")]
            peer_channels: BTreeMap::new(),
            #[cfg(feature = "cluster")]
            local_frontier: InputFrontier::default(),
            #[cfg(feature = "cluster")]
            last_broadcast: InputFrontier::default(),
            #[cfg(feature = "cluster")]
            effective_frontier: InputFrontier::default(),
            #[cfg(feature = "cluster")]
            remote_peer_cursor: None,
            #[cfg(feature = "cluster")]
            queued_payload_bytes: 0,
            #[cfg(feature = "cluster")]
            queued_event_capacity_bytes: 0,
            #[cfg(feature = "cluster")]
            queued_remote_events: 0,
            #[cfg(feature = "cluster")]
            pending_cluster_input: None,
            #[cfg(feature = "cluster")]
            whole_restore_applied: false,
            #[cfg(feature = "cluster")]
            prepared_vnode_transition: None,
            #[cfg(feature = "cluster")]
            vnode_transition_cleanup: None,
        }
    }

    fn local_full_aggregate_cache_enabled(&self) -> bool {
        let QueryState::Agg(aggregate) = &self.state else {
            return false;
        };
        if self.emit_changelog || aggregate.having_filter().is_some() {
            return false;
        }
        #[cfg(feature = "cluster")]
        {
            self.cluster_shuffle.is_none()
        }
        #[cfg(not(feature = "cluster"))]
        {
            true
        }
    }

    fn invalidate_local_aggregate_output_cache(&mut self) {
        self.cached_local_aggregate_output = None;
    }

    fn local_aggregate_output_cache_bytes(&self) -> usize {
        self.cached_local_aggregate_output
            .as_ref()
            .map_or(0, |cached| cached.retained_bytes)
    }

    fn clone_cached_local_aggregate_output(&self) -> Option<Vec<RecordBatch>> {
        if !self.local_full_aggregate_cache_enabled() {
            return None;
        }
        self.cached_local_aggregate_output
            .as_ref()
            .map(|cached| cached.batches.clone())
    }

    fn try_cache_local_aggregate_output(&mut self, batches: &[RecordBatch]) {
        self.invalidate_local_aggregate_output_cache();
        if !self.local_full_aggregate_cache_enabled() {
            return;
        }
        let retained_batches = batches.to_vec();
        let Some(retained_bytes) = retained_batches
            .capacity()
            .checked_mul(std::mem::size_of::<RecordBatch>())
            .and_then(|roster_bytes| {
                retained_batches
                    .iter()
                    .try_fold(roster_bytes, |total, batch| {
                        batch
                            .num_columns()
                            .checked_mul(std::mem::size_of::<arrow::array::ArrayRef>())
                            .and_then(|columns| total.checked_add(columns))
                            .and_then(|total| total.checked_add(batch.get_array_memory_size()))
                    })
            })
        else {
            return;
        };
        let QueryState::Agg(aggregate) = &self.state else {
            return;
        };
        let Some(accounted_with_cache) = aggregate
            .accounted_state_bytes()
            .checked_add(retained_bytes)
        else {
            return;
        };
        if accounted_with_cache > self.max_managed_state_bytes {
            return;
        }
        self.cached_local_aggregate_output = Some(CachedLocalAggregateOutput {
            batches: retained_batches,
            retained_bytes,
        });
    }

    fn drop_local_aggregate_cache_over_budget(&mut self) {
        let Some(cached) = self.cached_local_aggregate_output.as_ref() else {
            return;
        };
        let QueryState::Agg(aggregate) = &self.state else {
            self.invalidate_local_aggregate_output_cache();
            return;
        };
        if aggregate
            .accounted_state_bytes()
            .checked_add(cached.retained_bytes)
            .is_none_or(|accounted| accounted > self.max_managed_state_bytes)
        {
            self.invalidate_local_aggregate_output_cache();
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn attach_cluster_shuffle(&mut self, config: ClusterShuffleConfig) {
        self.invalidate_local_aggregate_output_cache();
        debug_assert!(self.cluster_shuffle.is_none());
        debug_assert_eq!(
            config.registry.vnode_count(),
            u32::from(self.key_group_count)
        );
        let assignment = config.registry.versioned_snapshot();
        let peers = Self::remote_owner_peers(&assignment, config.self_id);
        self.cluster_assignment_digest = Some(self.owner_map_digest(&assignment));
        self.peer_channels = peers
            .iter()
            .copied()
            .map(|peer| (peer, AggPeerChannel::default()))
            .collect();
        self.cluster_peers = peers.into();
        self.cluster_assignment = Some(assignment);
        self.cluster_shuffle = Some(config);
    }

    #[allow(clippy::too_many_lines)]
    async fn lazy_init(&mut self) -> Result<(), DbError> {
        self.invalidate_local_aggregate_output_cache();
        if let Some(agg_state) = IncrementalAggState::try_from_sql(
            &self.ctx,
            &self.sql,
            self.emit_changelog,
            self.key_group_count,
        )
        .await?
        {
            if self.emit_changelog && agg_state.having_filter().is_some() {
                return Err(DbError::Pipeline(format!(
                    "aggregate '{}' cannot use HAVING with changelog output until transition-aware HAVING retractions are implemented",
                    self.op_name
                )));
            }
            #[cfg(feature = "cluster")]
            if self.cluster_shuffle.is_some() {
                let expected_state_class = if agg_state.num_group_cols() == 0 {
                    OperatorStateClass::GlobalSingleton
                } else {
                    OperatorStateClass::VnodeKeyed
                };
                if self.capability.managed_state != Some(ManagedStateContract::SqlAggregateV1)
                    || self.capability.state_class != expected_state_class
                {
                    return Err(DbError::Pipeline(format!(
                        "[{}] query '{}': initialized aggregate state does not match its immutable cluster capability ({:?}, {:?})",
                        laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED,
                        self.op_name,
                        self.capability.state_class,
                        self.capability.managed_state
                    )));
                }
            }
            self.log_execution_path(agg_state.compiled_projection().is_some());
            self.state = QueryState::Agg(Box::new(agg_state));
            return Ok(());
        }

        let df = self
            .ctx
            .sql(&self.sql)
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
        let mut plan = df.logical_plan().clone();

        if crate::aggregate_state::find_aggregate(&plan).is_some() {
            return Err(DbError::Unsupported(format!(
                "[{}] query '{}': aggregate cannot use the generic DataFusion path; the incremental execution path was not constructed",
                laminar_core::error_codes::SQL_UNSUPPORTED,
                self.op_name
            )));
        }

        let single_source = single_source_table(&self.sql).is_some();
        let weighted_projection = single_source
            && extract_projection_filter(&plan).is_some_and(|projection| {
                let schema = projection.input_df_schema.as_arrow();
                let weight = laminar_core::changelog::WEIGHT_COLUMN;
                schema.fields().last().is_some_and(|field| {
                    field.name() == weight
                        && field.data_type() == &arrow::datatypes::DataType::Int64
                        && !field.is_nullable()
                }) && schema
                    .fields()
                    .iter()
                    .filter(|field| field.name().eq_ignore_ascii_case(weight))
                    .count()
                    == 1
            });
        let execution_sql = if weighted_projection {
            projection_sql_preserving_weight(&self.sql).ok_or_else(|| {
                DbError::Pipeline(format!(
                    "query '{}' cannot preserve its canonical changelog weight through one SQL projection",
                    self.op_name
                ))
            })?
        } else {
            self.sql.clone()
        };
        if weighted_projection {
            plan = self
                .ctx
                .sql(&execution_sql)
                .await
                .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?
                .logical_plan()
                .clone();
        }

        if single_source {
            if let Some(proj) = self.try_build_compiled_projection(&plan) {
                tracing::debug!(
                    query = %self.op_name,
                    "Non-aggregate single-source query compiled to PhysicalExpr"
                );
                if weighted_projection {
                    self.sql.clone_from(&execution_sql);
                }
                self.log_execution_path(true);
                self.state = QueryState::Compiled(proj);
                return Ok(());
            }
            let physical = self
                .ctx
                .state()
                .create_physical_plan(&plan)
                .await
                .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
            if weighted_projection {
                self.sql.clone_from(&execution_sql);
            }
            self.log_execution_path(false);
            self.state = QueryState::CachedPlan(physical);
        } else {
            let physical = self
                .ctx
                .state()
                .create_physical_plan(&plan)
                .await
                .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
            self.log_execution_path(false);
            self.state = QueryState::CachedPhysical(physical);
        }
        Ok(())
    }

    fn log_execution_path(&mut self, compiled: bool) {
        if self.execution_path_logged {
            return;
        }
        self.execution_path_logged = true;
        if let Some(ref m) = self.prom {
            if compiled {
                m.queries_compiled.inc();
            } else {
                m.queries_cached_plan.inc();
            }
        }
    }

    fn try_build_compiled_projection(
        &self,
        plan: &datafusion_expr::LogicalPlan,
    ) -> Option<CompiledProjection> {
        let info = extract_projection_filter(plan)?;
        let state = self.ctx.state();
        let props = state.execution_props();
        let mut compiled_exprs = Vec::with_capacity(info.proj_exprs.len());
        let mut proj_fields = Vec::with_capacity(info.proj_exprs.len());

        for expr in &info.proj_exprs {
            let phys =
                datafusion::physical_expr::create_physical_expr(expr, &info.input_df_schema, props)
                    .ok()?;
            let dt = phys.data_type(info.input_df_schema.as_arrow()).ok()?;
            let nullable = phys
                .nullable(info.input_df_schema.as_arrow())
                .unwrap_or(true);
            let name = match expr {
                datafusion_expr::Expr::Column(col) => col.name.clone(),
                datafusion_expr::Expr::Alias(alias) => alias.name.clone(),
                _ => expr.schema_name().to_string(),
            };
            proj_fields.push(arrow::datatypes::Field::new(name, dt, nullable));
            compiled_exprs.push(phys);
        }

        let compiled_filter = if let Some(ref pred) = info.filter_predicate {
            Some(
                datafusion::physical_expr::create_physical_expr(pred, &info.input_df_schema, props)
                    .ok()?,
            )
        } else {
            None
        };

        let output_schema = Arc::new(arrow::datatypes::Schema::new(proj_fields));
        Some(CompiledProjection {
            exprs: compiled_exprs,
            filter: compiled_filter,
            output_schema,
        })
    }

    async fn build_and_cache_physical_plan(&mut self) -> Result<(), DbError> {
        let df = self
            .ctx
            .sql(&self.sql)
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
        let plan = df.logical_plan().clone();
        let physical = self
            .ctx
            .state()
            .create_physical_plan(&plan)
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
        self.state = QueryState::CachedPlan(physical);
        Ok(())
    }

    async fn execute_cached_plan(&self) -> Result<Vec<RecordBatch>, DbError> {
        let QueryState::CachedPlan(ref plan) = self.state else {
            return Err(DbError::Pipeline(
                "internal: execute_cached_plan called on non-CachedPlan state".into(),
            ));
        };
        datafusion::physical_plan::collect(plan.clone(), self.task_ctx.clone())
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))
    }

    async fn pre_aggregate(&mut self, inputs: &[RecordBatch]) -> Result<Vec<RecordBatch>, DbError> {
        let QueryState::Agg(ref mut agg_state) = self.state else {
            return Err(DbError::Pipeline(
                "internal: pre_aggregate called on non-agg state".into(),
            ));
        };

        let batches = if let Some(proj) = agg_state.compiled_projection() {
            match try_evaluate_compiled(proj, inputs) {
                Ok(result) => result,
                Err(e) => {
                    tracing::debug!(
                        query = %self.op_name,
                        error = %e,
                        "Compiled pre-agg projection failed, falling back to cached plan"
                    );
                    if let Some(physical) = agg_state.cached_pre_agg_physical() {
                        super::execute_cached_physical(
                            self.task_ctx.clone(),
                            &self.op_name,
                            physical,
                        )
                        .await?
                    } else {
                        return Err(DbError::Pipeline(format!(
                            "[LDB-8051] query '{}': compiled pre-agg failed and no cached plan: {e}",
                            self.op_name
                        )));
                    }
                }
            }
        } else if let Some(physical) = agg_state.cached_pre_agg_physical() {
            super::execute_cached_physical(self.task_ctx.clone(), &self.op_name, physical).await?
        } else {
            return Err(DbError::Pipeline(format!(
                "[LDB-8050] query '{}': no compiled projection or cached plan",
                self.op_name
            )));
        };
        Ok(batches)
    }

    fn prepare_local_aggregate_batches(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        match &self.state {
            QueryState::Agg(aggregate) if aggregate.certifies_local_input_coalescing() => {
                coalesce_local_aggregate_batches(&self.op_name, batches)
            }
            QueryState::Agg(_) => Ok(batches),
            _ => Err(DbError::Pipeline(
                "internal: local aggregate input preparation targeted non-aggregate state".into(),
            )),
        }
    }

    fn apply_routed_aggregate(
        &mut self,
        batches: &[(RecordBatch, Option<u32>)],
        watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        // Any routed apply may change accumulator or tombstone state. Drop the reusable full-state
        // emission before the first mutation so partial-apply failures cannot leave a stale cache.
        self.invalidate_local_aggregate_output_cache();
        let QueryState::Agg(ref mut aggregate) = self.state else {
            return Err(DbError::Pipeline(
                "internal: routed aggregate apply targeted non-aggregate state".into(),
            ));
        };
        for (batch, vnode) in batches {
            aggregate.process_batch_for_vnode(batch, watermark, *vnode)?;
        }
        self.emit_agg_output()
    }

    async fn execute_agg(
        &mut self,
        inputs: &[RecordBatch],
        watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        // Finish projection and all coalescing fallible work before aggregate state can change.
        let batches = self.pre_aggregate(inputs).await?;
        let batches = self.prepare_local_aggregate_batches(batches)?;
        let routed = batches
            .into_iter()
            .map(|batch| (batch, None))
            .collect::<Vec<_>>();
        let output = self
            .apply_routed_aggregate(&routed, watermark)
            .map_err(|error| {
                stateful_apply_outcome_unknown(&self.op_name, "state update or output", error)
            })?;
        // `emit_agg_output` has already applied HAVING. Cache only this successful final result;
        // caching is an optional optimization and silently declines on accounting/budget limits.
        self.try_cache_local_aggregate_output(&output);
        Ok(output)
    }

    #[cfg(feature = "cluster")]
    fn frontier_watermark(frontier: InputFrontier) -> i64 {
        frontier.watermark.unwrap_or(i64::MIN)
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
    fn validate_frontier(
        &self,
        previous: InputFrontier,
        next: InputFrontier,
        context: &str,
    ) -> Result<(), DbError> {
        if next.watermark == Some(i64::MIN)
            || (previous.watermark.is_some() && next.watermark.is_none())
            || matches!((previous.watermark, next.watermark), (Some(previous), Some(next)) if next < previous)
        {
            return Err(DbError::ShuffleTerminal(format!(
                "aggregate '{}' {context} frontier regressed or became uninitialized",
                self.op_name
            )));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn remote_owner_peers(assignment: &VnodeAssignmentSnapshot, self_id: NodeId) -> Vec<u64> {
        assignment
            .owners()
            .iter()
            .copied()
            .filter(|owner| !owner.is_unassigned() && *owner != self_id)
            .map(|owner| owner.0)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    #[cfg(feature = "cluster")]
    fn owner_map_digest(&self, assignment: &VnodeAssignmentSnapshot) -> [u8; 32] {
        let owners = assignment
            .owners()
            .iter()
            .map(|owner| owner.0)
            .collect::<Vec<_>>();
        laminar_core::checkpoint::CheckpointAssignmentFence::owner_map_digest(
            u32::from(self.key_group_count),
            &owners,
        )
    }

    #[cfg(feature = "cluster")]
    fn portable_handoff_cut(
        &self,
        transition: &ManagedVnodeTransition<'_>,
        expected_donors: &[u64],
    ) -> Result<Option<InputFrontier>, DbError> {
        if transition.whole_restores.len() != expected_donors.len()
            || !transition
                .whole_restores
                .iter()
                .map(|restore| restore.participant_id)
                .eq(expected_donors.iter().copied())
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' portable whole donors do not exactly match acquired-state donors {expected_donors:?}",
                self.op_name
            )));
        }
        if expected_donors.is_empty() {
            return Ok(None);
        }

        let predecessor_participants = &transition.predecessor.participants;
        let mut common = None;
        for restore in transition.whole_restores {
            let cut = with_aligned_aggregate_checkpoint_bytes(restore.state, |state| {
                let checkpoint = rkyv::access::<ArchivedAggOpCheckpoint, rkyv::rancor::Error>(
                    state,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "aggregate '{}' donor {} whole checkpoint archive: {error}",
                        self.op_name, restore.participant_id
                    ))
                })?;
                if checkpoint.version != AGG_OP_CHECKPOINT_VERSION
                    || checkpoint.assignment_version.to_native()
                        != transition.predecessor.assignment_version
                    || checkpoint.owner_map_digest != transition.predecessor.assignment_digest
                    || checkpoint.self_id.to_native() != restore.participant_id
                {
                    return Err(DbError::Checkpoint(format!(
                        "aggregate '{}' donor {} whole checkpoint is outside the portable predecessor cut",
                        self.op_name, restore.participant_id
                    )));
                }

                let expected_peer_count = predecessor_participants.len().saturating_sub(1);
                // The v2 archive deliberately omits `accepted`: capture reconstructs the
                // accepted frontier from `applied` plus the serialized event tail and rejects a
                // mismatch. An empty validated tail is therefore exact accepted == applied
                // evidence, not merely an absence of payload bytes.
                if checkpoint.channels.len() != expected_peer_count
                    || !checkpoint
                        .channels
                        .iter()
                        .map(|channel| channel.peer.to_native())
                        .eq(predecessor_participants.iter().filter_map(|participant| {
                            (participant.node_id != restore.participant_id)
                                .then_some(participant.node_id)
                        }))
                    || checkpoint
                        .channels
                        .iter()
                        .any(|channel| !channel.events.is_empty())
                    || checkpoint.remote_peer_cursor.as_ref().is_some_and(|peer| {
                        let peer = peer.to_native();
                        peer == restore.participant_id || !transition.predecessor.contains(peer)
                    })
                {
                    return Err(DbError::Checkpoint(format!(
                        "aggregate '{}' donor {} portable channel roster is invalid",
                        self.op_name, restore.participant_id
                    )));
                }

                let local = archived_agg_checkpoint_frontier(&checkpoint.local_frontier);
                let effective = archived_agg_checkpoint_frontier(&checkpoint.effective_frontier);
                self.validate_frontier(InputFrontier::default(), local, "portable local")?;
                self.validate_frontier(InputFrontier::default(), effective, "portable effective")?;
                for channel in checkpoint.channels.iter() {
                    self.validate_frontier(
                        InputFrontier::default(),
                        archived_agg_checkpoint_frontier(&channel.applied),
                        "portable remote",
                    )?;
                }
                let merged = merge_input_frontier_iter(
                    std::iter::once(local).chain(
                        checkpoint
                            .channels
                            .iter()
                            .map(|channel| archived_agg_checkpoint_frontier(&channel.applied)),
                    ),
                    i64::MIN,
                );
                if merged != effective {
                    return Err(DbError::Checkpoint(format!(
                        "aggregate '{}' donor {} portable frontiers do not form one drained cut",
                        self.op_name, restore.participant_id
                    )));
                }
                Ok(effective)
            })?;
            if common.is_some_and(|expected| expected != cut) {
                return Err(DbError::Checkpoint(format!(
                    "aggregate '{}' donor whole checkpoints disagree on the handoff frontier",
                    self.op_name
                )));
            }
            common = Some(cut);
        }
        Ok(common)
    }

    #[cfg(feature = "cluster")]
    fn active_cluster_scope(
        &self,
    ) -> Result<(ClusterShuffleConfig, VnodeAssignmentSnapshot, Arc<[u64]>), DbError> {
        let config = self.cluster_shuffle.clone().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "aggregate '{}' has no cluster shuffle scope",
                self.op_name
            ))
        })?;
        let pinned = self.cluster_assignment.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "aggregate '{}' has no pinned cluster assignment",
                self.op_name
            ))
        })?;
        self.cluster_assignment_digest.ok_or_else(|| {
            DbError::Checkpoint(format!(
                "aggregate '{}' has no pinned assignment digest",
                self.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let sender_digest = config.sender.active_assignment_digest();
        if u32::try_from(assignment.owners().len()).ok() != Some(u32::from(self.key_group_count))
            || assignment.version() != pinned.version()
            || !std::ptr::eq(assignment.owners(), pinned.owners())
            || sender_digest.is_none()
            || sender_digest != config.receiver.active_assignment_digest()
            || config.sender.local_id() != config.self_id.0
            || config.receiver.local_id() != config.self_id.0
            || config.sender.incarnation() != config.receiver.incarnation()
            || config.sender.assignment_version() != assignment.version()
            || config.receiver.assignment_version() != assignment.version()
            || config.sender.recovery_gen() != config.receiver.recovery_gen()
        {
            return Err(DbError::ShuffleNotReady(format!(
                "aggregate '{}' cluster ownership is outside its attached assignment",
                self.op_name
            )));
        }
        Ok((config, assignment, Arc::clone(&self.cluster_peers)))
    }

    #[cfg(feature = "cluster")]
    fn accounting_error(&self) -> DbError {
        DbError::Pipeline(format!(
            "aggregate '{}' state accounting overflow",
            self.op_name
        ))
    }

    #[cfg(feature = "cluster")]
    fn cluster_topology_bytes(&self) -> Result<usize, DbError> {
        let peers = self
            .cluster_peers
            .len()
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| self.accounting_error())?;
        let channels = self
            .peer_channels
            .len()
            .checked_mul(
                std::mem::size_of::<(u64, AggPeerChannel)>() + AGG_PEER_CHANNEL_ENTRY_CHARGE,
            )
            .and_then(|bytes| bytes.checked_add(self.queued_event_capacity_bytes))
            .ok_or_else(|| self.accounting_error())?;
        peers
            .checked_add(channels)
            .ok_or_else(|| self.accounting_error())
    }

    #[cfg(feature = "cluster")]
    fn checked_live_state_bytes(&self) -> Result<usize, DbError> {
        let aggregate = match &self.state {
            QueryState::Agg(aggregate) => aggregate.accounted_state_bytes(),
            _ => 0,
        };
        aggregate
            .checked_add(self.cluster_topology_bytes()?)
            .and_then(|bytes| bytes.checked_add(self.queued_payload_bytes))
            .and_then(|bytes| bytes.checked_add(self.local_aggregate_output_cache_bytes()))
            .and_then(|bytes| {
                bytes.checked_add(
                    self.pending_cluster_input
                        .as_ref()
                        .map_or(0, |pending| pending.accounted_bytes),
                )
            })
            .ok_or_else(|| self.accounting_error())
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
    fn cluster_input_plan_bytes(&self, plan: &AggClusterInputPlan) -> Result<usize, DbError> {
        let mut bytes = plan
            .local_batches
            .capacity()
            .checked_mul(std::mem::size_of::<(RecordBatch, Option<u32>)>())
            .and_then(|local| {
                plan.outbound
                    .capacity()
                    .checked_mul(std::mem::size_of::<(u64, ShuffleMessage)>())
                    .and_then(|outbound| local.checked_add(outbound))
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
        for (batch, _) in &plan.local_batches {
            bytes = bytes
                .checked_add(self.batch_plan_bytes(batch)?)
                .ok_or_else(|| self.accounting_error())?;
        }
        for (_, message) in &plan.outbound {
            let message_bytes = match message {
                ShuffleMessage::Barrier(_) => 0,
                ShuffleMessage::Frontier { stage, .. } => stage.capacity(),
                ShuffleMessage::Data {
                    stage,
                    routed_vnodes,
                    batch,
                } => self
                    .batch_plan_bytes(batch)?
                    .checked_add(stage.capacity())
                    .and_then(|bytes| {
                        routed_vnodes
                            .len()
                            .checked_mul(std::mem::size_of::<u32>())
                            .and_then(|routes| bytes.checked_add(routes))
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
    fn reserve_remote_event_slot(
        &mut self,
        peer: u64,
        payload_bytes: usize,
    ) -> Result<(), DbError> {
        let current = self.checked_live_state_bytes()?;
        let previous_capacity = self.peer_channels[&peer].events.capacity();
        self.peer_channels
            .get_mut(&peer)
            .expect("validated aggregate peer channel")
            .events
            .try_reserve_exact(1)
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "aggregate '{}' could not reserve ordered shuffle event: {error}",
                    self.op_name
                ))
            })?;
        let capacity = self.peer_channels[&peer].events.capacity();
        let added_capacity_bytes = capacity
            .checked_sub(previous_capacity)
            .and_then(|slots| slots.checked_mul(AGG_REMOTE_EVENT_CHARGE))
            .ok_or_else(|| self.accounting_error())?;
        let next = current
            .checked_add(added_capacity_bytes)
            .and_then(|bytes| bytes.checked_add(payload_bytes))
            .ok_or_else(|| self.accounting_error())?;
        if next > self.max_managed_state_bytes {
            self.peer_channels
                .get_mut(&peer)
                .expect("reserved aggregate peer channel")
                .events
                .shrink_to(previous_capacity);
            let retained_capacity = self.peer_channels[&peer]
                .events
                .capacity()
                .saturating_sub(previous_capacity)
                .checked_mul(AGG_REMOTE_EVENT_CHARGE)
                .ok_or_else(|| self.accounting_error())?;
            self.queued_event_capacity_bytes = self
                .queued_event_capacity_bytes
                .checked_add(retained_capacity)
                .ok_or_else(|| self.accounting_error())?;
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("aggregate '{}' ordered shuffle queue", self.op_name),
                accounted_bytes: next,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        self.queued_event_capacity_bytes = self
            .queued_event_capacity_bytes
            .checked_add(added_capacity_bytes)
            .ok_or_else(|| self.accounting_error())?;
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn expected_pre_aggregate_schema(&self) -> Result<arrow::datatypes::SchemaRef, DbError> {
        let QueryState::Agg(aggregate) = &self.state else {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' received shuffle data before initialization",
                self.op_name
            )));
        };
        if let Some(projection) = aggregate.compiled_projection() {
            return Ok(Arc::clone(&projection.output_schema));
        }
        aggregate.cached_pre_agg_physical().map_or_else(
            || {
                Err(DbError::Pipeline(format!(
                    "aggregate '{}' has no pre-aggregate schema",
                    self.op_name
                )))
            },
            |physical| Ok(physical.schema()),
        )
    }

    #[cfg(feature = "cluster")]
    fn build_queued_batch(
        &self,
        retained: crate::operator::RetainedBatch,
        accepted: InputFrontier,
        assignment: &VnodeAssignmentSnapshot,
        self_id: NodeId,
    ) -> Result<AggQueuedBatch, DbError> {
        if accepted.idle {
            return Err(DbError::ShuffleTerminal(format!(
                "aggregate '{}' received data while its peer channel was idle",
                self.op_name
            )));
        }
        let batch = retained.batch();
        if batch.num_rows() == 0
            || batch.num_rows() > laminar_core::shuffle::ROUTE_MAX_BATCH_ROWS
            || retained.routed_vnodes().is_empty()
            || retained
                .routed_vnodes()
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(DbError::ShuffleTerminal(format!(
                "aggregate '{}' rejected non-canonical shuffle data",
                self.op_name
            )));
        }
        let logical_bytes = laminar_core::shuffle::logical_batch_bytes(batch).map_err(|error| {
            DbError::ShuffleTerminal(format!(
                "aggregate '{}' rejected shuffle batch size: {error}",
                self.op_name
            ))
        })?;
        if logical_bytes > laminar_core::shuffle::ROUTE_MAX_BATCH_BYTES {
            return Err(DbError::ShuffleTerminal(format!(
                "aggregate '{}' shuffle batch exceeds its route limit",
                self.op_name
            )));
        }
        if batch.schema().as_ref() != self.expected_pre_aggregate_schema()?.as_ref() {
            return Err(DbError::ShuffleTerminal(format!(
                "aggregate '{}' shuffle schema does not match its pre-aggregate schema",
                self.op_name
            )));
        }
        if retained.routed_vnodes().iter().any(|vnode| {
            assignment
                .owners()
                .get(*vnode as usize)
                .is_none_or(|owner| *owner != self_id)
        }) {
            return Err(DbError::ShuffleTerminal(format!(
                "aggregate '{}' received data outside its vnode ownership",
                self.op_name
            )));
        }
        let QueryState::Agg(aggregate) = &self.state else {
            unreachable!("aggregate schema validation requires aggregate state");
        };
        let row_vnodes = hash_rows_to_vnodes(
            batch,
            aggregate.num_group_cols(),
            u32::from(self.key_group_count),
        )
        .map_err(|error| {
            crate::operator::shuffle_routing_error(
                &format!("aggregate [{}] received routing", self.op_name),
                &error,
            )
        })?;
        let mut seen = vec![false; retained.routed_vnodes().len()];
        for vnode in &row_vnodes {
            let Ok(index) = retained.routed_vnodes().binary_search(vnode) else {
                return Err(DbError::ShuffleTerminal(format!(
                    "aggregate '{}' shuffle vnode metadata omits a decoded row",
                    self.op_name
                )));
            };
            seen[index] = true;
        }
        if seen.iter().any(|seen| !seen) {
            return Err(DbError::ShuffleTerminal(format!(
                "aggregate '{}' shuffle vnode metadata names an absent row",
                self.op_name
            )));
        }
        let charged_bytes = retained
            .heap_bytes()
            .and_then(|bytes| bytes.checked_add(AGG_RETAINED_BATCH_ARC_CHARGE))
            .and_then(|bytes| {
                row_vnodes
                    .len()
                    .checked_mul(std::mem::size_of::<u32>())
                    .and_then(|vnodes| vnodes.checked_add(AGG_ROW_VNODE_ARC_CHARGE))
                    .and_then(|vnodes| bytes.checked_add(vnodes))
            })
            .ok_or_else(|| self.accounting_error())?;
        Ok(AggQueuedBatch {
            retained: Arc::new(retained),
            row_vnodes: row_vnodes.into(),
            charged_bytes,
        })
    }

    #[cfg(feature = "cluster")]
    fn cluster_cycle_local_frontier(
        &self,
        supplied: InputFrontier,
        has_input: bool,
    ) -> Result<InputFrontier, DbError> {
        if self.last_broadcast == self.local_frontier {
            return Ok(supplied);
        }
        if has_input {
            return Err(DbError::InvalidOperation(format!(
                "aggregate '{}' received local input before its restored frontier was broadcast",
                self.op_name
            )));
        }
        Ok(self.local_frontier)
    }

    #[cfg(feature = "cluster")]
    fn normalized_local_frontier(
        &self,
        input: InputFrontier,
        has_data: bool,
    ) -> Result<InputFrontier, DbError> {
        if input.idle && has_data {
            return Err(DbError::InvalidOperation(format!(
                "aggregate '{}' received data from an idle local channel",
                self.op_name
            )));
        }
        let mut normalized = input;
        if self.local_frontier.idle {
            normalized.watermark = Self::max_watermark(
                normalized.watermark,
                if normalized.idle {
                    self.local_frontier.watermark
                } else {
                    Self::max_watermark(
                        self.local_frontier.watermark,
                        self.effective_frontier.watermark,
                    )
                },
            );
        }
        self.validate_frontier(self.local_frontier, normalized, "local")?;
        Ok(normalized)
    }

    #[cfg(feature = "cluster")]
    fn effective_cluster_frontier(
        &self,
        local: InputFrontier,
        consumed: Option<(u64, InputFrontier, bool)>,
    ) -> Result<InputFrontier, DbError> {
        let remote = self.peer_channels.iter().map(|(&peer, channel)| {
            let (mut applied, pending) = if let Some((target, applied, pending)) = consumed {
                if target == peer {
                    (applied, pending)
                } else {
                    (channel.applied, !channel.events.is_empty())
                }
            } else {
                (channel.applied, !channel.events.is_empty())
            };
            if pending {
                applied.idle = false;
                applied.watermark =
                    Self::max_watermark(applied.watermark, self.effective_frontier.watermark);
            }
            applied
        });
        let merged = merge_input_frontier_iter(std::iter::once(local).chain(remote), i64::MIN);
        self.validate_frontier(self.effective_frontier, merged, "effective")?;
        if self.pending_cluster_input.is_some() {
            return Ok(InputFrontier {
                watermark: self.effective_frontier.watermark,
                idle: false,
            });
        }
        Ok(merged)
    }

    #[cfg(feature = "cluster")]
    fn plan_cluster_batches(
        &self,
        batches: Vec<RecordBatch>,
        local_frontier: InputFrontier,
        config: &ClusterShuffleConfig,
        assignment: &VnodeAssignmentSnapshot,
        peers: &[u64],
    ) -> Result<AggClusterInputPlan, DbError> {
        let QueryState::Agg(aggregate) = &self.state else {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' is not initialized",
                self.op_name
            )));
        };
        let mut local_batches = Vec::new();
        let mut remote_data = BTreeMap::<u64, Vec<ShuffleMessage>>::new();
        for batch in batches.into_iter().filter(|batch| batch.num_rows() != 0) {
            let row_vnodes = hash_rows_to_vnodes(
                &batch,
                aggregate.num_group_cols(),
                u32::from(self.key_group_count),
            )
            .map_err(|error| {
                crate::operator::shuffle_routing_error(
                    &format!("aggregate [{}] routing", self.op_name),
                    &error,
                )
            })?;
            let plan = laminar_core::shuffle::route_checkpointed_batch(
                &batch,
                &row_vnodes,
                assignment,
                config.self_id,
            )
            .map_err(|error| {
                crate::operator::shuffle_routing_error(
                    &format!("aggregate [{}] routing", self.op_name),
                    &error,
                )
            })?;
            local_batches.extend(
                plan.local
                    .into_iter()
                    .map(|route| (route.batch, Some(route.vnode))),
            );
            for route in plan.remote {
                remote_data.entry(route.owner.0).or_default().push(
                    ShuffleMessage::checkpointed_routed(
                        self.op_name.to_string(),
                        route.routed_vnodes,
                        route.batch,
                    ),
                );
            }
        }
        let mut outbound = Vec::new();
        for &peer in peers {
            let data = remote_data.remove(&peer);
            let has_data = data.as_ref().is_some_and(|messages| !messages.is_empty());
            if has_data && self.last_broadcast.idle && !local_frontier.idle {
                outbound.push((
                    peer,
                    ShuffleMessage::Frontier {
                        stage: self.op_name.to_string(),
                        watermark: self.last_broadcast.watermark,
                        idle: false,
                    },
                ));
            }
            if let Some(messages) = data {
                outbound.extend(messages.into_iter().map(|message| (peer, message)));
            }
            if has_data || self.last_broadcast != local_frontier {
                outbound.push((
                    peer,
                    ShuffleMessage::Frontier {
                        stage: self.op_name.to_string(),
                        watermark: local_frontier.watermark,
                        idle: local_frontier.idle,
                    },
                ));
            }
        }
        if !remote_data.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' routed data outside its peer roster",
                self.op_name
            )));
        }
        let effective_frontier = self.effective_cluster_frontier(local_frontier, None)?;
        Ok(AggClusterInputPlan {
            local_batches,
            outbound,
            local_frontier,
            effective_frontier,
        })
    }

    #[cfg(feature = "cluster")]
    fn next_remote_peer(&self) -> Option<u64> {
        if self.cluster_peers.is_empty() {
            return None;
        }
        let start = self.remote_peer_cursor.map_or(0, |cursor| {
            let next = self.cluster_peers.partition_point(|peer| *peer <= cursor);
            if next == self.cluster_peers.len() {
                0
            } else {
                next
            }
        });
        (0..self.cluster_peers.len())
            .map(|offset| self.cluster_peers[(start + offset) % self.cluster_peers.len()])
            .find(|peer| {
                self.peer_channels
                    .get(peer)
                    .is_some_and(|channel| !channel.events.is_empty())
            })
    }

    #[cfg(feature = "cluster")]
    fn remote_replay_error(&self, error: DbError) -> DbError {
        if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
            error
        } else {
            DbError::Checkpoint(format!(
                "aggregate '{}' ordered shuffle replay requires recovery: {error}",
                self.op_name
            ))
        }
    }

    #[cfg(feature = "cluster")]
    fn drain_remote_event(
        &mut self,
        assignment: &VnodeAssignmentSnapshot,
        config: &ClusterShuffleConfig,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let peer = self.next_remote_peer().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "aggregate '{}' remote event accounting is inconsistent",
                self.op_name
            ))
        })?;
        let event = self.peer_channels[&peer]
            .events
            .front()
            .cloned()
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' selected an empty peer queue",
                    self.op_name
                ))
            })?;
        if event.assignment_version != assignment.version() {
            return Err(self.remote_replay_error(DbError::Checkpoint(format!(
                "aggregate '{}' replay crossed its assignment or recovery boundary",
                self.op_name
            ))));
        }
        let channel = &self.peer_channels[&peer];
        let (local_batches, applied) = match &event.payload {
            AggRemoteEventPayload::Data(batch) => {
                if channel.applied.idle {
                    return Err(self.remote_replay_error(DbError::ShuffleTerminal(format!(
                        "aggregate '{}' queued data directly behind an idle frontier",
                        self.op_name
                    ))));
                }
                let plan = laminar_core::shuffle::route_checkpointed_batch(
                    batch.retained.batch(),
                    &batch.row_vnodes,
                    assignment,
                    config.self_id,
                )
                .map_err(|error| {
                    self.remote_replay_error(crate::operator::shuffle_routing_error(
                        &format!("aggregate [{}] queued routing", self.op_name),
                        &error,
                    ))
                })?;
                if !plan.remote.is_empty() {
                    return Err(self.remote_replay_error(DbError::Checkpoint(format!(
                        "aggregate '{}' queued data is no longer locally owned",
                        self.op_name
                    ))));
                }
                (
                    plan.local
                        .into_iter()
                        .map(|route| (route.batch, Some(route.vnode)))
                        .collect::<Vec<_>>(),
                    channel.applied,
                )
            }
            AggRemoteEventPayload::Frontier(frontier) => {
                self.validate_frontier(channel.applied, *frontier, "remote applied")
                    .map_err(|error| self.remote_replay_error(error))?;
                (Vec::new(), *frontier)
            }
        };
        let pending = channel.events.len() > 1;
        let effective = self
            .effective_cluster_frontier(self.local_frontier, Some((peer, applied, pending)))
            .map_err(|error| self.remote_replay_error(error))?;
        let output = if local_batches.is_empty() {
            Vec::new()
        } else {
            self.apply_routed_aggregate(&local_batches, Self::frontier_watermark(effective))
                .map_err(|error| self.remote_replay_error(error))?
        };
        let released = event.payload_bytes();
        let channel = self
            .peer_channels
            .get_mut(&peer)
            .expect("planned aggregate peer channel");
        channel
            .events
            .pop_front()
            .expect("planned aggregate remote event");
        if matches!(event.payload, AggRemoteEventPayload::Frontier(_)) {
            channel.applied = applied;
        }
        self.queued_payload_bytes = self
            .queued_payload_bytes
            .checked_sub(released)
            .expect("aggregate queue accounting was prevalidated");
        self.queued_remote_events = self
            .queued_remote_events
            .checked_sub(1)
            .expect("aggregate event accounting was prevalidated");
        self.remote_peer_cursor = Some(peer);
        self.effective_frontier = effective;
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
            .expect("aggregate send plan must be installed before it starts");
        debug_assert!(pending.send.is_none());
        debug_assert!(pending.completion.is_none());
        let outbound = pending
            .outbound
            .take()
            .expect("idle aggregate send plan must retain its outbound cut");
        let sender = Arc::clone(&config.sender);
        let wake = config.receiver.work_ready_notify();
        let context = format!("aggregate [{}] shuffle", self.op_name);
        let (completion_tx, completion) = tokio::sync::oneshot::channel();
        pending.completion = Some(completion);
        pending.send = Some(tokio::spawn(async move {
            let outcome = crate::operator::send_shuffle_plan_retaining(
                &sender,
                assignment_version,
                outbound,
                &context,
            )
            .await;
            let should_wake = !matches!(&outcome.0, Err(error) if error.is_shuffle_not_ready());
            if completion_tx.send(outcome).is_ok() && should_wake {
                wake.notify_one();
            }
        }));
    }

    #[cfg(feature = "cluster")]
    fn outbound_finalize_error(&self, error: DbError) -> DbError {
        if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
            error
        } else {
            DbError::ShufflePartialSend(format!(
                "aggregate '{}' failed after outbound shuffle admission: {error}",
                self.op_name
            ))
        }
    }

    #[cfg(feature = "cluster")]
    fn finish_pending_cluster_input(&mut self) -> Result<PendingAggCompletion, DbError> {
        let outcome = {
            let Some(pending) = self.pending_cluster_input.as_mut() else {
                return Ok(PendingAggCompletion::Waiting);
            };
            match (pending.send.as_ref(), pending.completion.as_mut()) {
                (None, None) => return Ok(PendingAggCompletion::Waiting),
                (Some(_), Some(completion)) => match completion.try_recv() {
                    Ok(outcome) => Ok(outcome),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                        return Ok(PendingAggCompletion::Waiting);
                    }
                    Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                        Err("send task ended without a delivery outcome")
                    }
                },
                _ => Err("send task lost its completion channel"),
            }
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(reason) => {
                drop(self.pending_cluster_input.take());
                return Err(DbError::ShufflePartialSend(format!(
                    "aggregate '{}' {reason}",
                    self.op_name
                )));
            }
        };
        let mut pending = self
            .pending_cluster_input
            .take()
            .expect("finished aggregate send plan");
        pending.send.take().expect("completed aggregate send task");
        pending
            .completion
            .take()
            .expect("completed aggregate send outcome");
        let (result, outbound) = outcome;
        if let Err(error) = result {
            if error.is_shuffle_not_ready() {
                pending.outbound = Some(outbound.ok_or_else(|| {
                    DbError::ShufflePartialSend(format!(
                        "aggregate '{}' safe send failure lost its retry plan",
                        self.op_name
                    ))
                })?);
                self.pending_cluster_input = Some(pending);
                return Ok(PendingAggCompletion::RetryLater);
            }
            return Err(error);
        }
        debug_assert!(outbound.is_none());
        let effective = self
            .effective_cluster_frontier(pending.local_frontier, None)
            .map_err(|error| self.outbound_finalize_error(error))?;
        let output = if pending.local_batches.is_empty() {
            Vec::new()
        } else {
            self.apply_routed_aggregate(&pending.local_batches, Self::frontier_watermark(effective))
                .map_err(|error| self.outbound_finalize_error(error))?
        };
        self.local_frontier = pending.local_frontier;
        self.last_broadcast = pending.local_frontier;
        self.effective_frontier = effective;
        Ok(PendingAggCompletion::Applied(output))
    }

    #[cfg(feature = "cluster")]
    async fn process_cluster(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontier: InputFrontier,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let scope = self.active_cluster_scope();
        let (config, assignment, peers) = match scope {
            Ok(scope) => scope,
            Err(error) if self.pending_cluster_input.is_some() => {
                return Err(self.outbound_finalize_error(error));
            }
            Err(error) => return Err(error),
        };
        let has_input = inputs
            .iter()
            .flat_map(|batches| batches.iter())
            .any(|batch| batch.num_rows() != 0);
        let mut output = Vec::new();
        let mut drained_remote = false;
        if self.queued_remote_events != 0 {
            if has_input {
                return Err(DbError::InvalidOperation(format!(
                    "aggregate '{}' received local input while ordered shuffle replay was pending",
                    self.op_name
                )));
            }
            output.extend(self.drain_remote_event(&assignment, &config)?);
            drained_remote = true;
        }
        let completion = self.finish_pending_cluster_input().map_err(|error| {
            if drained_remote {
                self.remote_replay_error(error)
            } else {
                error
            }
        })?;
        match completion {
            PendingAggCompletion::Applied(local) => {
                output.extend(local);
                return Ok(output);
            }
            PendingAggCompletion::Waiting | PendingAggCompletion::RetryLater => {}
        }
        if self.pending_cluster_input.is_some() {
            if has_input {
                return Err(
                    self.outbound_finalize_error(DbError::InvalidOperation(format!(
                        "aggregate '{}' received local input while a shuffle send was pending",
                        self.op_name
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
            return Ok(output);
        }
        if drained_remote {
            return Ok(output);
        }
        let input_batches = inputs.first().map_or(&[][..], Vec::as_slice);
        // A restored/transitioned topology deliberately leaves its exact local cut
        // unbroadcast, which makes `wants_input` hold graph-buffered rows. Do not let the
        // concurrently observed live source frontier leap over that cut during this
        // frontier-only bootstrap cycle. Once the cut is acknowledged, normal node-local
        // frontier advancement resumes on the next cycle.
        let frontier = self.cluster_cycle_local_frontier(frontier, has_input)?;
        let local_frontier = self.normalized_local_frontier(frontier, has_input)?;
        let pre_aggregate = self.pre_aggregate(input_batches).await?;
        let plan =
            self.plan_cluster_batches(pre_aggregate, local_frontier, &config, &assignment, &peers)?;
        if !plan.outbound.is_empty() {
            let accounted_bytes = self.cluster_input_plan_bytes(&plan)?;
            let total = self
                .checked_live_state_bytes()?
                .checked_add(accounted_bytes)
                .ok_or_else(|| self.accounting_error())?;
            if total > self.max_managed_state_bytes {
                return Err(DbError::ManagedStateBudgetExceeded {
                    context: format!("aggregate '{}' pending shuffle send", self.op_name),
                    accounted_bytes: total,
                    limit_bytes: self.max_managed_state_bytes,
                });
            }
            let AggClusterInputPlan {
                local_batches,
                outbound,
                local_frontier,
                effective_frontier: _,
            } = plan;
            self.pending_cluster_input = Some(PendingAggClusterInput {
                local_batches,
                outbound: Some(outbound),
                local_frontier,
                send: None,
                completion: None,
                accounted_bytes,
            });
            self.start_pending_cluster_send(&config, assignment.version());
            return Ok(Vec::new());
        }
        let output = if plan.local_batches.is_empty() {
            Vec::new()
        } else {
            self.apply_routed_aggregate(
                &plan.local_batches,
                Self::frontier_watermark(plan.effective_frontier),
            )
            .map_err(|error| {
                stateful_apply_outcome_unknown(&self.op_name, "state update or output", error)
            })?
        };
        self.local_frontier = plan.local_frontier;
        self.last_broadcast = plan.local_frontier;
        self.effective_frontier = plan.effective_frontier;
        Ok(output)
    }

    #[cfg(feature = "cluster")]
    fn capture_cluster_checkpoint(
        &self,
        max_capture_bytes: u64,
    ) -> Result<AggCheckpointCapture, DbError> {
        let (config, assignment, peers) = self.active_cluster_scope()?;
        if self.cluster_assignment_digest != Some(self.owner_map_digest(&assignment)) {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' checkpoint assignment digest is inconsistent",
                self.op_name
            )));
        }
        if self.pending_cluster_input.is_some()
            || self.last_broadcast != self.local_frontier
            || self.peer_channels.len() != peers.len()
            || self
                .remote_peer_cursor
                .is_some_and(|peer| peers.binary_search(&peer).is_err())
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' shuffle topology is not at a checkpoint boundary",
                self.op_name
            )));
        }
        let effective = self.effective_cluster_frontier(self.local_frontier, None)?;
        let has_queued = self.queued_remote_events != 0;
        if effective.watermark != self.effective_frontier.watermark
            || (!has_queued && effective != self.effective_frontier)
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' effective frontier is not at its retained channel cut",
                self.op_name
            )));
        }
        let mut requested_retained = std::mem::size_of::<AggCheckpointCapture>()
            .checked_add(
                peers
                    .len()
                    .checked_mul(std::mem::size_of::<CapturedAggChannel>())
                    .ok_or_else(|| self.accounting_error())?,
            )
            .ok_or_else(|| self.accounting_error())?;
        let mut requested_payload_bytes = 0usize;
        let mut requested_events = 0usize;
        for &peer in peers.iter() {
            let channel = self.peer_channels.get(&peer).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' checkpoint is missing peer {peer}",
                    self.op_name
                ))
            })?;
            requested_retained = requested_retained
                .checked_add(
                    channel
                        .events
                        .len()
                        .checked_mul(std::mem::size_of::<CapturedAggEvent>())
                        .ok_or_else(|| self.accounting_error())?,
                )
                .ok_or_else(|| self.accounting_error())?;
            requested_events = requested_events
                .checked_add(channel.events.len())
                .ok_or_else(|| self.accounting_error())?;
            for event in &channel.events {
                let payload_bytes = event.payload_bytes();
                requested_payload_bytes = requested_payload_bytes
                    .checked_add(payload_bytes)
                    .ok_or_else(|| self.accounting_error())?;
                requested_retained = requested_retained
                    .checked_add(payload_bytes)
                    .ok_or_else(|| self.accounting_error())?;
            }
        }
        if requested_payload_bytes != self.queued_payload_bytes
            || requested_events != self.queued_remote_events
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' ordered shuffle accounting is inconsistent",
                self.op_name
            )));
        }
        let requested_retained =
            u64::try_from(requested_retained).map_err(|_| self.accounting_error())?;
        if requested_retained > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' channel capture requires {requested_retained} bytes; capture headroom is {max_capture_bytes} bytes",
                self.op_name
            )));
        }
        let mut channels = Vec::new();
        channels.try_reserve_exact(peers.len()).map_err(|error| {
            DbError::Checkpoint(format!(
                "aggregate '{}' checkpoint could not reserve channel metadata: {error}",
                self.op_name
            ))
        })?;
        let mut retained = std::mem::size_of::<AggCheckpointCapture>()
            .checked_add(
                channels
                    .capacity()
                    .checked_mul(std::mem::size_of::<CapturedAggChannel>())
                    .ok_or_else(|| self.accounting_error())?,
            )
            .and_then(|bytes| bytes.checked_add(self.queued_payload_bytes))
            .ok_or_else(|| self.accounting_error())?;
        let mut queued_payload_bytes = 0usize;
        let mut queued_event_capacity_bytes = 0usize;
        let mut queued_remote_events = 0usize;
        let recovery_gen = config.receiver.recovery_gen();
        for &peer in peers.iter() {
            let channel = self.peer_channels.get(&peer).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' checkpoint is missing peer {peer}",
                    self.op_name
                ))
            })?;
            let mut events = Vec::new();
            events
                .try_reserve_exact(channel.events.len())
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "aggregate '{}' checkpoint could not reserve peer events: {error}",
                        self.op_name
                    ))
                })?;
            retained = retained
                .checked_add(
                    events
                        .capacity()
                        .checked_mul(std::mem::size_of::<CapturedAggEvent>())
                        .ok_or_else(|| self.accounting_error())?,
                )
                .ok_or_else(|| self.accounting_error())?;
            queued_event_capacity_bytes = queued_event_capacity_bytes
                .checked_add(
                    channel
                        .events
                        .capacity()
                        .checked_mul(AGG_REMOTE_EVENT_CHARGE)
                        .ok_or_else(|| self.accounting_error())?,
                )
                .ok_or_else(|| self.accounting_error())?;
            let mut accepted = channel.applied;
            let mut previous_recovery = None;
            for event in &channel.events {
                if event.assignment_version != assignment.version()
                    || event.recovery_gen > recovery_gen
                    || previous_recovery.is_some_and(|previous| event.recovery_gen < previous)
                {
                    return Err(DbError::Checkpoint(format!(
                        "aggregate '{}' checkpoint queue crossed its active transport scope",
                        self.op_name
                    )));
                }
                previous_recovery = Some(event.recovery_gen);
                queued_payload_bytes = queued_payload_bytes
                    .checked_add(event.payload_bytes())
                    .ok_or_else(|| self.accounting_error())?;
                queued_remote_events = queued_remote_events
                    .checked_add(1)
                    .ok_or_else(|| self.accounting_error())?;
                events.push(match &event.payload {
                    AggRemoteEventPayload::Data(batch) => {
                        if accepted.idle {
                            return Err(DbError::Checkpoint(format!(
                                "aggregate '{}' checkpoint contains data behind an idle peer frontier",
                                self.op_name
                            )));
                        }
                        if batch.retained.assignment_version()
                            != Some(event.assignment_version)
                            || batch.retained.peer() != Some(peer)
                            || batch.retained.recovery_gen() != Some(event.recovery_gen)
                        {
                            return Err(DbError::Checkpoint(format!(
                                "aggregate '{}' checkpoint data identity does not match its channel event",
                                self.op_name
                            )));
                        }
                        CapturedAggEvent::Data {
                            recovery_gen: event.recovery_gen,
                            retained: Arc::clone(&batch.retained),
                        }
                    }
                    AggRemoteEventPayload::Frontier(frontier) => {
                        if accepted.idle
                            && !frontier.idle
                            && frontier.watermark
                                != Self::max_watermark(
                                    frontier.watermark,
                                    self.effective_frontier.watermark,
                                )
                        {
                            return Err(DbError::Checkpoint(format!(
                                "aggregate '{}' checkpoint peer revival precedes its effective frontier",
                                self.op_name
                            )));
                        }
                        self.validate_frontier(accepted, *frontier, "checkpoint queued")?;
                        accepted = *frontier;
                        CapturedAggEvent::Frontier {
                            recovery_gen: event.recovery_gen,
                            frontier: *frontier,
                        }
                    }
                });
            }
            if accepted != channel.accepted {
                return Err(DbError::Checkpoint(format!(
                    "aggregate '{}' checkpoint peer frontier does not match its event tail",
                    self.op_name
                )));
            }
            channels.push(CapturedAggChannel {
                peer,
                applied: channel.applied,
                events,
            });
        }
        if queued_payload_bytes != self.queued_payload_bytes
            || queued_event_capacity_bytes != self.queued_event_capacity_bytes
            || queued_remote_events != self.queued_remote_events
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' ordered shuffle accounting is inconsistent",
                self.op_name
            )));
        }
        let retained_bytes = u64::try_from(retained).map_err(|_| self.accounting_error())?;
        if retained_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' channel capture retains {retained_bytes} bytes; capture headroom is {max_capture_bytes} bytes",
                self.op_name
            )));
        }
        Ok(AggCheckpointCapture {
            assignment_version: assignment.version(),
            owner_map_digest: self
                .cluster_assignment_digest
                .expect("active aggregate assignment has a digest"),
            self_id: config.self_id.0,
            recovery_gen,
            local_frontier: self.local_frontier,
            effective_frontier: self.effective_frontier,
            remote_peer_cursor: self.remote_peer_cursor,
            channels,
            retained_bytes,
        })
    }

    fn emit_agg_output(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        let QueryState::Agg(ref mut agg_state) = self.state else {
            return Err(DbError::Pipeline(
                "internal: emit_agg_output on non-agg".into(),
            ));
        };

        let mut batches = agg_state.emit()?;

        if let Some(filter) = agg_state.having_filter() {
            batches = apply_compiled_having(&batches, filter)?;
        }

        Ok(batches)
    }
}

#[cfg(feature = "cluster")]
pub(crate) fn hash_rows_to_vnodes(
    batch: &RecordBatch,
    num_group_cols: usize,
    vnode_count: u32,
) -> Result<Vec<u32>, laminar_core::shuffle::ShuffleRoutingError> {
    if num_group_cols == 0 || batch.num_rows() == 0 {
        return Ok(vec![0; batch.num_rows()]);
    }
    let columns: Vec<usize> = (0..num_group_cols).collect();
    laminar_core::shuffle::row_vnodes(batch, &columns, vnode_count)
}

#[async_trait]
impl GraphOperator for SqlQueryOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        debug_assert_eq!(
            self.capability.implementation,
            OperatorImplementation::SqlQuery
        );
        self.capability
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_aligned_replay_pending(&self) -> bool {
        self.pending_cluster_input.is_some() || self.queued_remote_events != 0
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        let QueryState::Agg(aggregate) = &self.state else {
            return None;
        };

        #[cfg(feature = "cluster")]
        let (prepared_bytes, retired_bytes) = {
            let staged = self
                .prepared_vnode_transition
                .as_ref()
                .map_or(0, |prepared| {
                    prepared
                        .aggregate
                        .accounted_state_bytes()
                        .saturating_add(prepared.topology.accounted_state_bytes())
                });
            match self.vnode_transition_cleanup.as_ref() {
                Some(SqlVnodeTransitionCleanup::Aborted(prepared)) => (
                    staged
                        .saturating_add(prepared.aggregate.accounted_state_bytes())
                        .saturating_add(prepared.topology.accounted_state_bytes()),
                    0,
                ),
                Some(SqlVnodeTransitionCleanup::Published {
                    aggregate,
                    topology,
                }) => (
                    staged,
                    aggregate
                        .accounted_state_bytes()
                        .saturating_add(topology.accounted_state_bytes()),
                ),
                None => (staged, 0),
            }
        };
        #[cfg(not(feature = "cluster"))]
        let (prepared_bytes, retired_bytes) = (0, 0);

        #[cfg(feature = "cluster")]
        let live = self.checked_live_state_bytes().unwrap_or(usize::MAX);
        #[cfg(not(feature = "cluster"))]
        let live = aggregate
            .accounted_state_bytes()
            .saturating_add(self.local_aggregate_output_cache_bytes());
        #[cfg(feature = "cluster")]
        let _ = aggregate;

        Some(ManagedStateAccountingSnapshot {
            live,
            prepared: prepared_bytes,
            retired: retired_bytes,
        })
    }

    fn evict_optional_managed_state(&mut self) -> usize {
        let evicted = self.local_aggregate_output_cache_bytes();
        self.invalidate_local_aggregate_output_cache();
        evicted
    }

    fn set_managed_state_budget(&mut self, bytes: usize) {
        self.max_managed_state_bytes = bytes;
        self.drop_local_aggregate_cache_over_budget();
    }

    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        if matches!(self.state, QueryState::Uninit) {
            self.lazy_init().await?;
        }
        if matches!(self.state, QueryState::Agg(_)) {
            #[cfg(feature = "cluster")]
            if self.cluster_shuffle.is_some() {
                let accounted = self.checked_live_state_bytes()?;
                if accounted > self.max_managed_state_bytes {
                    return Err(DbError::ManagedStateBudgetExceeded {
                        context: format!("aggregate '{}' topology", self.op_name),
                        accounted_bytes: accounted,
                        limit_bytes: self.max_managed_state_bytes,
                    });
                }
            }
            return Ok(());
        }
        // The immutable AST classifier can over-approximate function syntax. Only an initialized
        // incremental aggregate owns managed state.
        self.capability.managed_state = None;
        Ok(())
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if matches!(self.state, QueryState::Uninit) {
            self.lazy_init().await?;
        }
        #[cfg(feature = "cluster")]
        if self.cluster_shuffle.is_some() && matches!(self.state, QueryState::Agg(_)) {
            let watermark = watermarks.first().copied().unwrap_or(i64::MIN);
            return self
                .process_cluster(
                    inputs,
                    InputFrontier {
                        watermark: (watermark != i64::MIN).then_some(watermark),
                        idle: false,
                    },
                )
                .await;
        }

        let watermark = watermarks.first().copied().unwrap_or(i64::MIN);

        let input_batches = inputs.first().map_or(&[] as &[RecordBatch], Vec::as_slice);

        if input_batches.is_empty() || input_batches.iter().all(|b| b.num_rows() == 0) {
            if matches!(self.state, QueryState::Agg(_)) {
                if let Some(cached) = self.clone_cached_local_aggregate_output() {
                    return Ok(cached);
                }
                return self.execute_agg(input_batches, watermark).await;
            }
            return Ok(Vec::new());
        }

        match &self.state {
            QueryState::Uninit => unreachable!("lazy_init already called"),
            QueryState::Agg(_) => self.execute_agg(input_batches, watermark).await,
            QueryState::Compiled(_) => {
                let QueryState::Compiled(ref proj) = self.state else {
                    unreachable!();
                };
                match try_evaluate_compiled(proj, input_batches) {
                    Ok(result) => Ok(result),
                    Err(e) => {
                        tracing::debug!(
                            query = %self.op_name,
                            error = %e,
                            "Compiled projection failed, falling back to cached plan"
                        );
                        self.build_and_cache_physical_plan().await?;
                        self.execute_cached_plan().await
                    }
                }
            }
            QueryState::CachedPlan(_) => match self.execute_cached_plan().await {
                Ok(batches) => Ok(batches),
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("Schema error") || err_str.contains("schema mismatch") {
                        tracing::debug!(
                            query = %self.op_name,
                            error = %e,
                            "Cached physical plan invalidated, re-planning"
                        );
                        self.build_and_cache_physical_plan().await?;
                        self.execute_cached_plan().await
                    } else {
                        Err(e)
                    }
                }
            },
            QueryState::CachedPhysical(ref physical) => {
                super::execute_cached_physical(self.task_ctx.clone(), &self.op_name, physical).await
            }
        }
    }

    async fn process_with_frontiers(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: &[InputFrontier],
    ) -> Result<Vec<RecordBatch>, DbError> {
        #[cfg(feature = "cluster")]
        if self.cluster_shuffle.is_some() {
            if frontiers.len() != 1 {
                return Err(DbError::InvalidOperation(format!(
                    "aggregate '{}' requires one input frontier",
                    self.op_name
                )));
            }
            if matches!(self.state, QueryState::Uninit) {
                self.lazy_init().await?;
            }
            if matches!(self.state, QueryState::Agg(_)) {
                return self.process_cluster(inputs, frontiers[0]).await;
            }
        }
        let watermarks = frontiers
            .iter()
            .map(|frontier| frontier.watermark.unwrap_or(i64::MIN))
            .collect::<Vec<_>>();
        self.process(inputs, &watermarks).await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        #[cfg(not(feature = "cluster"))]
        {
            Ok(None)
        }

        #[cfg(feature = "cluster")]
        {
            if self.cluster_shuffle.is_none() || !matches!(self.state, QueryState::Agg(_)) {
                return Ok(None);
            }
            let capture = self.capture_cluster_checkpoint(u64::MAX)?;
            let state = encode_agg_checkpoint_capture(&self.op_name, capture, usize::MAX)?;
            Ok(Some(OperatorCheckpoint {
                data: state.bytes().to_vec(),
            }))
        }
    }

    fn checkpoint_capture(
        &mut self,
        max_capture_bytes: u64,
    ) -> Result<Option<StateFrameCapture>, DbError> {
        #[cfg(not(feature = "cluster"))]
        {
            let _ = max_capture_bytes;
            Ok(None)
        }

        #[cfg(feature = "cluster")]
        {
            if self.cluster_shuffle.is_none() || !matches!(self.state, QueryState::Agg(_)) {
                return Ok(None);
            }
            let capture = self.capture_cluster_checkpoint(max_capture_bytes)?;
            let retained_bytes = capture.retained_bytes;
            let op_name = Arc::clone(&self.op_name);
            Ok(Some(StateFrameCapture::deferred(
                retained_bytes,
                move |max_working_bytes| {
                    encode_agg_checkpoint_capture(op_name.as_ref(), capture, max_working_bytes)
                },
            )))
        }
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        #[cfg(not(feature = "cluster"))]
        {
            let _ = checkpoint;
            Err(DbError::Checkpoint(format!(
                "aggregate '{}' checkpoint contains cluster channel state without cluster support",
                self.op_name
            )))
        }

        #[cfg(feature = "cluster")]
        {
            self.invalidate_local_aggregate_output_cache();
            if !matches!(self.state, QueryState::Agg(_)) {
                return Err(DbError::Checkpoint(format!(
                    "aggregate '{}' channel restore targeted non-aggregate state",
                    self.op_name
                )));
            }
            let OperatorCheckpoint { data } = checkpoint;
            let restore_bytes = data
                .len()
                .checked_add(aggregate_checkpoint_alignment_copy_bytes(&data))
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "aggregate '{}' channel checkpoint restore accounting overflow",
                        self.op_name
                    ))
                })?;
            if restore_bytes > self.max_managed_state_bytes {
                return Err(DbError::ManagedStateBudgetExceeded {
                    context: format!("aggregate '{}' channel checkpoint restore", self.op_name),
                    accounted_bytes: restore_bytes,
                    limit_bytes: self.max_managed_state_bytes,
                });
            }
            let checkpoint = with_aligned_aggregate_checkpoint_bytes(&data, |data| {
                rkyv::from_bytes::<AggOpCheckpoint, rkyv::rancor::Error>(data).map_err(|error| {
                    DbError::Checkpoint(format!(
                        "checkpoint deserialization for '{}': {error}",
                        self.op_name
                    ))
                })
            })?;
            drop(data);
            let (config, assignment, peers) = self.active_cluster_scope()?;
            let pristine = !self.whole_restore_applied
                && self.pending_cluster_input.is_none()
                && self.local_frontier == InputFrontier::default()
                && self.last_broadcast == InputFrontier::default()
                && self.effective_frontier == InputFrontier::default()
                && self.remote_peer_cursor.is_none()
                && self.queued_payload_bytes == 0
                && self.queued_event_capacity_bytes == 0
                && self.queued_remote_events == 0
                && self.peer_channels.values().all(|channel| {
                    channel.applied == InputFrontier::default()
                        && channel.accepted == InputFrontier::default()
                        && channel.events.is_empty()
                });
            if !pristine {
                return Err(DbError::Checkpoint(format!(
                    "aggregate '{}' channel checkpoint was restored more than once or after processing",
                    self.op_name
                )));
            }
            if checkpoint.version != AGG_OP_CHECKPOINT_VERSION
                || checkpoint.assignment_version != assignment.version()
                || checkpoint.owner_map_digest != self.owner_map_digest(&assignment)
                || checkpoint.self_id != config.self_id.0
                || checkpoint.recovery_gen > config.receiver.recovery_gen()
                || checkpoint.channels.len() != peers.len()
                || checkpoint
                    .remote_peer_cursor
                    .is_some_and(|peer| peers.binary_search(&peer).is_err())
            {
                return Err(DbError::Checkpoint(format!(
                    "aggregate '{}' channel checkpoint does not match its active assignment",
                    self.op_name
                )));
            }
            let local_frontier: InputFrontier = checkpoint.local_frontier.into();
            let effective_frontier: InputFrontier = checkpoint.effective_frontier.into();
            let checkpoint_recovery_gen = checkpoint.recovery_gen;
            self.validate_frontier(InputFrontier::default(), local_frontier, "restored local")?;
            self.validate_frontier(
                InputFrontier::default(),
                effective_frontier,
                "restored effective",
            )?;
            let mut decoded = BTreeMap::new();
            let mut queued_payload_bytes = 0usize;
            let mut queued_event_capacity_bytes = 0usize;
            let mut queued_remote_events = 0usize;
            let mut previous_peer = None;
            for channel in checkpoint.channels {
                if previous_peer.is_some_and(|previous| channel.peer <= previous)
                    || peers.binary_search(&channel.peer).is_err()
                    || decoded.contains_key(&channel.peer)
                {
                    return Err(DbError::Checkpoint(format!(
                        "aggregate '{}' checkpoint peer roster is not canonical",
                        self.op_name
                    )));
                }
                previous_peer = Some(channel.peer);
                let applied: InputFrontier = channel.applied.into();
                self.validate_frontier(InputFrontier::default(), applied, "restored remote")?;
                let mut cursor = applied;
                let mut previous_recovery = None;
                let mut events = VecDeque::new();
                events
                    .try_reserve_exact(channel.events.len())
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "aggregate '{}' restore could not reserve channel events: {error}",
                            self.op_name
                        ))
                    })?;
                queued_event_capacity_bytes = queued_event_capacity_bytes
                    .checked_add(
                        events
                            .capacity()
                            .checked_mul(AGG_REMOTE_EVENT_CHARGE)
                            .ok_or_else(|| self.accounting_error())?,
                    )
                    .ok_or_else(|| self.accounting_error())?;
                for event in channel.events {
                    let event = match event {
                        AggCheckpointEvent::Data {
                            recovery_gen,
                            routed_vnodes,
                            ipc,
                        } => {
                            if cursor.idle
                                || recovery_gen > checkpoint_recovery_gen
                                || previous_recovery.is_some_and(|previous| recovery_gen < previous)
                            {
                                return Err(DbError::Checkpoint(format!(
                                    "aggregate '{}' restored data outside an active peer frontier",
                                    self.op_name
                                )));
                            }
                            previous_recovery = Some(recovery_gen);
                            let batch = laminar_core::serialization::deserialize_batch_stream(&ipc)
                                .map_err(|error| {
                                    DbError::Checkpoint(format!(
                                        "aggregate '{}' channel restore IPC: {error}",
                                        self.op_name
                                    ))
                                })?;
                            let retained = crate::operator::RetainedBatch::restored_channel(
                                batch,
                                channel.peer,
                                assignment.version(),
                                recovery_gen,
                                routed_vnodes.into(),
                            );
                            let batch = self.build_queued_batch(
                                retained,
                                cursor,
                                &assignment,
                                config.self_id,
                            )?;
                            queued_payload_bytes = queued_payload_bytes
                                .checked_add(batch.charged_bytes)
                                .ok_or_else(|| self.accounting_error())?;
                            AggRemoteEvent {
                                assignment_version: assignment.version(),
                                recovery_gen,
                                payload: AggRemoteEventPayload::Data(batch),
                            }
                        }
                        AggCheckpointEvent::Frontier {
                            recovery_gen,
                            frontier,
                        } => {
                            if recovery_gen > checkpoint_recovery_gen
                                || previous_recovery.is_some_and(|previous| recovery_gen < previous)
                            {
                                return Err(DbError::Checkpoint(format!(
                                    "aggregate '{}' restored frontier crossed its recovery generation",
                                    self.op_name
                                )));
                            }
                            previous_recovery = Some(recovery_gen);
                            let frontier: InputFrontier = frontier.into();
                            if cursor.idle
                                && !frontier.idle
                                && frontier.watermark
                                    != Self::max_watermark(
                                        frontier.watermark,
                                        effective_frontier.watermark,
                                    )
                            {
                                return Err(DbError::Checkpoint(format!(
                                    "aggregate '{}' restored peer revival precedes its effective frontier",
                                    self.op_name
                                )));
                            }
                            self.validate_frontier(cursor, frontier, "restored queued")?;
                            cursor = frontier;
                            AggRemoteEvent {
                                assignment_version: assignment.version(),
                                recovery_gen,
                                payload: AggRemoteEventPayload::Frontier(frontier),
                            }
                        }
                    };
                    events.push_back(event);
                    queued_remote_events = queued_remote_events
                        .checked_add(1)
                        .ok_or_else(|| self.accounting_error())?;
                }
                decoded.insert(
                    channel.peer,
                    AggPeerChannel {
                        applied,
                        accepted: cursor,
                        events,
                    },
                );
            }
            if decoded.keys().copied().ne(peers.iter().copied()) {
                return Err(DbError::Checkpoint(format!(
                    "aggregate '{}' checkpoint peer roster is incomplete",
                    self.op_name
                )));
            }
            let merged = merge_input_frontier_iter(
                std::iter::once(local_frontier).chain(decoded.values().map(|channel| {
                    let mut applied = channel.applied;
                    if !channel.events.is_empty() {
                        applied.idle = false;
                        applied.watermark =
                            Self::max_watermark(applied.watermark, effective_frontier.watermark);
                    }
                    applied
                })),
                i64::MIN,
            );
            if merged.watermark != effective_frontier.watermark
                || (queued_remote_events == 0 && merged != effective_frontier)
            {
                return Err(DbError::Checkpoint(format!(
                    "aggregate '{}' restored channel frontiers do not form its effective cut",
                    self.op_name
                )));
            }
            let topology_bytes = peers
                .len()
                .checked_mul(std::mem::size_of::<u64>())
                .and_then(|bytes| {
                    decoded
                        .len()
                        .checked_mul(
                            std::mem::size_of::<(u64, AggPeerChannel)>()
                                + AGG_PEER_CHANNEL_ENTRY_CHARGE,
                        )
                        .and_then(|channels| bytes.checked_add(channels))
                })
                .and_then(|bytes| bytes.checked_add(queued_event_capacity_bytes))
                .and_then(|bytes| bytes.checked_add(queued_payload_bytes))
                .ok_or_else(|| self.accounting_error())?;
            let aggregate_bytes = match &self.state {
                QueryState::Agg(aggregate) => aggregate.accounted_state_bytes(),
                _ => 0,
            };
            let accounted = aggregate_bytes
                .checked_add(topology_bytes)
                .ok_or_else(|| self.accounting_error())?;
            if accounted > self.max_managed_state_bytes {
                return Err(DbError::ManagedStateBudgetExceeded {
                    context: format!("aggregate '{}' ordered shuffle restore", self.op_name),
                    accounted_bytes: accounted,
                    limit_bytes: self.max_managed_state_bytes,
                });
            }
            self.peer_channels = decoded;
            self.local_frontier = local_frontier;
            self.last_broadcast = local_frontier;
            self.effective_frontier = effective_frontier;
            self.remote_peer_cursor = checkpoint.remote_peer_cursor;
            self.queued_payload_bytes = queued_payload_bytes;
            self.queued_event_capacity_bytes = queued_event_capacity_bytes;
            self.queued_remote_events = queued_remote_events;
            self.whole_restore_applied = true;
            Ok(())
        }
    }

    #[cfg(feature = "cluster")]
    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        if self.cluster_shuffle.is_none() || !matches!(self.state, QueryState::Agg(_)) {
            return input;
        }
        let mut output = self.effective_frontier;
        if self.pending_cluster_input.is_some() || self.queued_remote_events != 0 {
            output.idle = false;
        }
        output
    }

    #[cfg(feature = "cluster")]
    fn restored_output_frontier(&self) -> Option<InputFrontier> {
        self.cluster_shuffle.as_ref()?;
        if !matches!(self.state, QueryState::Agg(_)) {
            return None;
        }
        let mut frontier = self.effective_frontier;
        if self.pending_cluster_input.is_some() || self.queued_remote_events != 0 {
            frontier.idle = false;
        }
        Some(frontier)
    }

    #[cfg(feature = "cluster")]
    fn wants_input(&self) -> bool {
        self.pending_cluster_input.is_none()
            && self.queued_remote_events == 0
            && self.last_broadcast == self.local_frontier
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_drain_pending(&self) -> bool {
        self.pending_cluster_input.is_some() || self.last_broadcast != self.local_frontier
    }

    #[cfg(feature = "cluster")]
    fn deferred_work_is_runnable(&self) -> bool {
        self.queued_remote_events != 0
            || (self.pending_cluster_input.is_none() && self.last_broadcast != self.local_frontier)
    }

    #[cfg(feature = "cluster")]
    fn advances_frontier_without_input(&self) -> bool {
        self.cluster_shuffle.is_some() && matches!(self.state, QueryState::Agg(_))
    }

    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle(
        &mut self,
        stage: &str,
        batch: crate::operator::RetainedBatch,
        _watermark: i64,
    ) -> Result<(), DbError> {
        if stage != self.op_name.as_ref() {
            return Err(DbError::ShuffleTerminal(format!(
                "aggregate '{}' rejected unknown shuffle stage '{stage}'",
                self.op_name
            )));
        }
        let (config, assignment, peers) = self.active_cluster_scope()?;
        let peer = batch.peer().ok_or_else(|| {
            DbError::ShuffleTerminal(format!(
                "aggregate '{}' received unscoped shuffle data",
                self.op_name
            ))
        })?;
        if peers.binary_search(&peer).is_err()
            || batch.assignment_version() != Some(assignment.version())
            || batch.recovery_gen() != Some(config.receiver.recovery_gen())
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' received data from peer {peer} outside assignment {} recovery {}",
                self.op_name,
                assignment.version(),
                config.receiver.recovery_gen()
            )));
        }
        let accepted = self.peer_channels[&peer].accepted;
        let batch = self.build_queued_batch(batch, accepted, &assignment, config.self_id)?;
        let next_payload = self
            .queued_payload_bytes
            .checked_add(batch.charged_bytes)
            .ok_or_else(|| self.accounting_error())?;
        let next_events = self
            .queued_remote_events
            .checked_add(1)
            .ok_or_else(|| self.accounting_error())?;
        self.reserve_remote_event_slot(peer, batch.charged_bytes)?;
        let assignment_version = batch
            .retained
            .assignment_version()
            .expect("validated aggregate assignment");
        let recovery_gen = batch
            .retained
            .recovery_gen()
            .expect("validated aggregate recovery generation");
        self.peer_channels
            .get_mut(&peer)
            .expect("reserved aggregate peer channel")
            .events
            .push_back(AggRemoteEvent {
                assignment_version,
                recovery_gen,
                payload: AggRemoteEventPayload::Data(batch),
            });
        self.queued_payload_bytes = next_payload;
        self.queued_remote_events = next_events;
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
        if stage != self.op_name.as_ref() {
            return Err(DbError::ShuffleTerminal(format!(
                "aggregate '{}' rejected unknown frontier stage '{stage}'",
                self.op_name
            )));
        }
        let (config, assignment, peers) = self.active_cluster_scope()?;
        if peers.binary_search(&peer).is_err()
            || assignment_version != assignment.version()
            || recovery_gen != config.receiver.recovery_gen()
            || frontier.watermark == Some(i64::MIN)
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' received frontier from peer {peer} outside assignment {} recovery {}",
                self.op_name,
                assignment.version(),
                config.receiver.recovery_gen()
            )));
        }
        let previous = self.peer_channels[&peer].accepted;
        if previous.watermark.is_some() && frontier.watermark.is_none() {
            self.validate_frontier(previous, frontier, "accepted remote")?;
        }
        let frontier = InputFrontier {
            watermark: Self::max_watermark(frontier.watermark, self.effective_frontier.watermark),
            ..frontier
        };
        self.validate_frontier(previous, frontier, "accepted remote")?;
        let next_events = self
            .queued_remote_events
            .checked_add(1)
            .ok_or_else(|| self.accounting_error())?;
        self.reserve_remote_event_slot(peer, 0)?;
        let channel = self
            .peer_channels
            .get_mut(&peer)
            .expect("reserved aggregate peer channel");
        channel.events.push_back(AggRemoteEvent {
            assignment_version,
            recovery_gen,
            payload: AggRemoteEventPayload::Frontier(frontier),
        });
        channel.accepted = frontier;
        self.queued_remote_events = next_events;
        Ok(())
    }

    fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
        max_capture_bytes: u64,
    ) -> Result<Option<Vec<CapturedVnodeState>>, DbError> {
        #[cfg(feature = "cluster")]
        if self.pending_cluster_input.is_some() || self.last_broadcast != self.local_frontier {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' cannot capture vnodes across pending shuffle work",
                self.op_name
            )));
        }
        self.invalidate_local_aggregate_output_cache();
        let QueryState::Agg(aggregate) = &mut self.state else {
            return Ok(None);
        };
        let checkpoints =
            aggregate.capture_checkpoint_vnodes(required_vnodes, vnode_count, max_capture_bytes)?;
        let mut captured = Vec::with_capacity(checkpoints.len());
        let empty_frame = Arc::new(std::sync::OnceLock::<bytes::Bytes>::new());
        for (vnode, checkpoint) in checkpoints {
            let retained_bytes = checkpoint.retained_bytes();
            let empty_frame = checkpoint.is_empty().then(|| Arc::clone(&empty_frame));
            let op_name = Arc::clone(&self.op_name);
            let state = StateFrameCapture::deferred(retained_bytes, move |max_encoded_bytes| {
                if let Some(encoded) = empty_frame.as_ref().and_then(|frame| frame.get()) {
                    return Ok(EncodedStateFrame::shared(encoded.clone()));
                }
                let checkpoint = checkpoint.encode(max_encoded_bytes)?;
                let retained_serialization_bytes = checkpoint.retained_serialization_bytes()?;
                let archive_budget = max_encoded_bytes
                    .checked_sub(retained_serialization_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "aggregate '{op_name}' intermediate checkpoint exhausted its frame budget"
                        ))
                    })?;
                let encoded = serialize_agg_cp(&checkpoint, &op_name, archive_budget)?;
                if let Some(empty_frame) = empty_frame {
                    let _ = empty_frame.set(encoded.bytes().clone());
                }
                Ok(encoded)
            });
            captured.push(CapturedVnodeState {
                vnode,
                state: Some(state),
            });
        }
        Ok(Some(captured))
    }

    fn restore_vnode(&mut self, vnode: u32, vnode_count: u32, state: &[u8]) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        if self.pending_cluster_input.is_some() || self.last_broadcast != self.local_frontier {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' cannot restore vnode {vnode} across pending shuffle work",
                self.op_name
            )));
        }
        self.invalidate_local_aggregate_output_cache();
        let restore_bytes = state
            .len()
            .checked_add(aggregate_checkpoint_alignment_copy_bytes(state))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' vnode {vnode} restore accounting overflow",
                    self.op_name
                ))
            })?;
        if restore_bytes > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("aggregate '{}' vnode {vnode} restore", self.op_name),
                accounted_bytes: restore_bytes,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        let QueryState::Agg(aggregate) = &self.state else {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' vnode restore requires initialized managed state",
                self.op_name
            )));
        };
        let profile = aggregate.vnode_archive_restore_profile();
        #[cfg(feature = "cluster")]
        let live_bytes = self.checked_live_state_bytes()?;
        #[cfg(not(feature = "cluster"))]
        let live_bytes = aggregate.accounted_state_bytes();
        let transition_roster_bytes = aggregate.vnode_transition_restore_roster_bytes(1, 0)?;
        let restore_preflight = with_aligned_aggregate_checkpoint_bytes(state, |state| {
            profile
                .preflight(
                    state,
                    format_args!("aggregate '{}' vnode {vnode}", self.op_name),
                )
                .map(|archive| archive.restore_preflight())
                .map_err(|error| DbError::Checkpoint(error.to_string()))
        })?;
        let accounted_bytes = live_bytes
            .checked_add(restore_bytes)
            .and_then(|bytes| bytes.checked_add(transition_roster_bytes))
            .and_then(|bytes| {
                restore_preflight
                    .sequential_decode_bytes()
                    .and_then(|decode| bytes.checked_add(decode))
            })
            .and_then(|bytes| bytes.checked_add(restore_preflight.final_state_upper_bytes()))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' vnode {vnode} restore accounting overflow",
                    self.op_name
                ))
            })?;
        if accounted_bytes > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("aggregate '{}' vnode {vnode} restore decode", self.op_name),
                accounted_bytes,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        let checkpoint = with_aligned_aggregate_checkpoint_bytes(state, |state| {
            profile
                .preflight(
                    state,
                    format_args!("aggregate '{}' vnode {vnode}", self.op_name),
                )
                .and_then(|archive| {
                    archive.deserialize(format_args!("aggregate '{}' vnode {vnode}", self.op_name))
                })
                .map_err(|error| DbError::Checkpoint(error.to_string()))
        })?;
        let QueryState::Agg(aggregate) = &mut self.state else {
            unreachable!("aggregate restore profile came from initialized aggregate state")
        };
        aggregate
            .restore_vnode(vnode, vnode_count, checkpoint)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' vnode {vnode} restore: {error}",
                    self.op_name
                ))
            })
    }

    #[cfg(feature = "cluster")]
    fn prepare_vnode_transition(
        &mut self,
        transition: ManagedVnodeTransition<'_>,
    ) -> Result<(), DbError> {
        self.invalidate_local_aggregate_output_cache();
        if self.prepared_vnode_transition.is_some() || self.vnode_transition_cleanup.is_some() {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' already owns vnode transition state",
                self.op_name
            )));
        }
        if self.pending_cluster_input.is_some()
            || self.queued_remote_events != 0
            || self.last_broadcast != self.local_frontier
            || self
                .peer_channels
                .values()
                .any(|channel| !channel.events.is_empty() || channel.accepted != channel.applied)
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' cannot transition across pending shuffle work",
                self.op_name
            )));
        }
        if self.effective_cluster_frontier(self.local_frontier, None)? != self.effective_frontier {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' cannot transition from an inconsistent frontier cut",
                self.op_name
            )));
        }
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "aggregate '{}' cannot prepare vnode state without cluster ownership",
                self.op_name
            ))
        })?;
        let QueryState::Agg(ref aggregate) = self.state else {
            return Err(DbError::Checkpoint(format!(
                "managed vnode transition for '{}' targeted a non-aggregate query",
                self.op_name
            )));
        };
        aggregate.validate_vnode_count(transition.target.vnode_count)?;

        let assignment = config.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        let installed = self.cluster_assignment.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "aggregate '{}' has no installed assignment",
                self.op_name
            ))
        })?;
        let installed_owners = installed
            .owners()
            .iter()
            .map(|owner| owner.0)
            .collect::<Vec<_>>();
        let checkpoint_bootstrap = match transition.mode {
            ManagedVnodeTransitionMode::Live => false,
            ManagedVnodeTransitionMode::CheckpointBootstrap { predecessor_owners } => {
                let predecessor = predecessor_owners
                    .iter()
                    .map(|owner| owner.0)
                    .collect::<Vec<_>>();
                if !transition.predecessor.matches_owner_map(&predecessor) {
                    return Err(DbError::Checkpoint(format!(
                        "aggregate '{}' checkpoint bootstrap has an invalid predecessor owner map",
                        self.op_name
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
        let target_contains_self = transition.target.contains(config.self_id.0);
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
            && !target_contains_self;
        if transition.target.vnode_count != config.registry.vnode_count()
            || transition.target.assignment_version != assignment.version()
            || !transition.target.matches_owner_map(&owners)
            || !version_edge_valid
            || config.sender.recovery_gen() != config.receiver.recovery_gen()
            || (target_contains_self && !active_transport)
            || (!target_contains_self && !inactive_transport)
            || if checkpoint_bootstrap {
                installed.version() != assignment.version()
                    || installed.owners() != assignment.owners()
            } else {
                transition.predecessor.assignment_version != installed.version()
                    || !transition.predecessor.matches_owner_map(&installed_owners)
            }
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' vnode transition target does not match assignment {}",
                self.op_name,
                assignment.version()
            )));
        }

        let predecessor_owners: &[NodeId] = match transition.mode {
            ManagedVnodeTransitionMode::Live => installed.owners(),
            ManagedVnodeTransitionMode::CheckpointBootstrap { predecessor_owners } => {
                predecessor_owners
            }
        };
        let predecessor_retained = !checkpoint_bootstrap
            && transition
                .predecessor
                .participant_incarnation(config.self_id.0)
                == Some(config.sender.incarnation());
        let payload_bytes = transition
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
        let alignment_copy_bytes = transition
            .restores
            .iter()
            .map(|restore| aggregate_checkpoint_alignment_copy_bytes(restore.state))
            .chain(
                transition
                    .whole_restores
                    .iter()
                    .map(|restore| aggregate_checkpoint_alignment_copy_bytes(restore.state)),
            )
            .max()
            .unwrap_or(0);
        let minimum_roster_scratch_bytes = aggregate_transition_roster_scratch_bytes(
            transition.predecessor.participants.len(),
            transition.restores.len(),
            transition.restores.len(),
        )
        .ok_or_else(|| self.accounting_error())?;
        let minimum_payload_phase_bytes = payload_bytes
            .checked_add(alignment_copy_bytes)
            .and_then(|bytes| bytes.checked_add(minimum_roster_scratch_bytes))
            .ok_or_else(|| self.accounting_error())?;
        if minimum_payload_phase_bytes > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("aggregate '{}' transition payload", self.op_name),
                accounted_bytes: minimum_payload_phase_bytes,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        let live_bytes = self.checked_live_state_bytes()?;
        let minimum_transport_peak = live_bytes
            .checked_add(minimum_payload_phase_bytes)
            .ok_or_else(|| self.accounting_error())?;
        if minimum_transport_peak > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("aggregate '{}' transition transport", self.op_name),
                accounted_bytes: minimum_transport_peak,
                limit_bytes: self.max_managed_state_bytes,
            });
        }

        let mut expected_donors = Vec::new();
        expected_donors
            .try_reserve_exact(transition.predecessor.participants.len())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' could not reserve portable donor roster: {error}",
                    self.op_name
                ))
            })?;
        for (vnode, owner) in assignment.owners().iter().enumerate() {
            if *owner != config.self_id
                || (predecessor_retained && predecessor_owners.get(vnode) == Some(&config.self_id))
            {
                continue;
            }
            let donor = predecessor_owners.get(vnode).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' predecessor owner map is incomplete",
                    self.op_name
                ))
            })?;
            if donor.is_unassigned() {
                return Err(DbError::Checkpoint(format!(
                    "aggregate '{}' acquired vnode {vnode} has no predecessor donor",
                    self.op_name
                )));
            }
            if let Err(position) = expected_donors.binary_search(&donor.0) {
                expected_donors.insert(position, donor.0);
            }
        }
        let fresh_acquirer = target_contains_self && !predecessor_retained;

        let mut preflighted = Vec::new();
        preflighted
            .try_reserve_exact(transition.restores.len())
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' could not reserve archive preflight metadata",
                    self.op_name
                ))
            })?;
        let mut restored_lower_bounds = Vec::new();
        restored_lower_bounds
            .try_reserve_exact(transition.restores.len())
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' could not reserve cardinality preflight metadata",
                    self.op_name
                ))
            })?;
        let roster_scratch_bytes = aggregate_transition_roster_scratch_bytes(
            expected_donors.capacity(),
            preflighted.capacity(),
            restored_lower_bounds.capacity(),
        )
        .ok_or_else(|| self.accounting_error())?;
        let payload_phase_bytes = payload_bytes
            .checked_add(alignment_copy_bytes)
            .and_then(|bytes| bytes.checked_add(roster_scratch_bytes))
            .ok_or_else(|| self.accounting_error())?;
        if payload_phase_bytes > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("aggregate '{}' transition payload", self.op_name),
                accounted_bytes: payload_phase_bytes,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        let transport_peak = live_bytes
            .checked_add(payload_phase_bytes)
            .ok_or_else(|| self.accounting_error())?;
        if transport_peak > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("aggregate '{}' transition transport", self.op_name),
                accounted_bytes: transport_peak,
                limit_bytes: self.max_managed_state_bytes,
            });
        }

        let handoff_cut = self.portable_handoff_cut(&transition, &expected_donors)?;
        if !fresh_acquirer
            && handoff_cut.is_some_and(|frontier| frontier != self.effective_frontier)
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' donor cut does not match the retained owner frontier",
                self.op_name
            )));
        }
        let transition_frontier = if fresh_acquirer {
            handoff_cut.ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aggregate '{}' fresh owner is missing its portable whole cut",
                    self.op_name
                ))
            })?
        } else {
            self.effective_frontier
        };

        let archive_profile = aggregate.vnode_archive_restore_profile();
        let mut replacement_state_upper_bytes = 0usize;
        let mut sequential_decode_peak_bytes = 0usize;
        for restore in transition.restores {
            let restore_preflight =
                with_aligned_aggregate_checkpoint_bytes(restore.state, |state| {
                    archive_profile
                        .preflight(
                            state,
                            format_args!(
                                "per-vnode state for '{}' vnode {}",
                                self.op_name, restore.vnode
                            ),
                        )
                        .map(|archive| archive.restore_preflight())
                })?;
            replacement_state_upper_bytes = replacement_state_upper_bytes
                .checked_add(restore_preflight.final_state_upper_bytes())
                .ok_or_else(|| self.accounting_error())?;
            sequential_decode_peak_bytes = sequential_decode_peak_bytes.max(
                restore_preflight
                    .sequential_decode_bytes()
                    .ok_or_else(|| self.accounting_error())?,
            );
            restored_lower_bounds.push((restore.vnode, restore_preflight.group_count()));
            preflighted.push((restore.vnode, restore.state, restore_preflight));
        }
        aggregate.preflight_vnode_transition_cardinality(
            transition.target.vnode_count,
            &restored_lower_bounds,
            transition.revoked,
        )?;

        let internal_roster_bytes = aggregate.vnode_transition_restore_roster_bytes(
            transition.restores.len(),
            transition.revoked.len(),
        )?;
        let target_peer_count = transition
            .target
            .participants
            .iter()
            .filter(|participant| participant.node_id != config.self_id.0)
            .count();
        let topology_upper_bytes = assignment
            .owners()
            .len()
            .checked_mul(std::mem::size_of::<NodeId>() + std::mem::size_of::<u64>())
            .and_then(|bytes| {
                target_peer_count
                    .checked_mul(std::mem::size_of::<u64>())
                    .and_then(|peers| bytes.checked_add(peers))
            })
            .and_then(|bytes| {
                target_peer_count
                    .checked_mul(
                        std::mem::size_of::<(u64, AggPeerChannel)>()
                            + AGG_PEER_CHANNEL_ENTRY_CHARGE,
                    )
                    .and_then(|channels| bytes.checked_add(channels))
            })
            .ok_or_else(|| self.accounting_error())?;
        let decode_peak_bytes = live_bytes
            .checked_add(payload_phase_bytes)
            .and_then(|bytes| bytes.checked_add(internal_roster_bytes))
            .and_then(|bytes| bytes.checked_add(topology_upper_bytes))
            .and_then(|bytes| bytes.checked_add(replacement_state_upper_bytes))
            .and_then(|bytes| bytes.checked_add(sequential_decode_peak_bytes))
            .ok_or_else(|| self.accounting_error())?;
        if decode_peak_bytes > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("aggregate '{}' transition decode", self.op_name),
                accounted_bytes: decode_peak_bytes,
                limit_bytes: self.max_managed_state_bytes,
            });
        }

        let owned_restores = preflighted.into_iter().map(|(vnode, bytes, preflight)| {
            let state = with_aligned_aggregate_checkpoint_bytes(bytes, |bytes| {
                let archive = archive_profile.preflight(
                    bytes,
                    format_args!("per-vnode state for '{}' vnode {vnode}", self.op_name),
                )?;
                debug_assert_eq!(archive.restore_preflight(), preflight);
                archive.deserialize(format_args!(
                    "per-vnode state for '{}' vnode {vnode}",
                    self.op_name
                ))
            })?;
            Ok(OwnedAggVnodeRestore { vnode, state })
        });
        let aggregate = aggregate.prepare_owned_vnode_transition(
            transition.target.vnode_count,
            owned_restores,
            transition.revoked,
        )?;
        let target_peers = Self::remote_owner_peers(&assignment, config.self_id);
        let mut channels = BTreeMap::new();
        for &peer in &target_peers {
            let same_incarnation = transition.predecessor.participant_incarnation(peer)
                == transition.target.participant_incarnation(peer);
            let channel = if fresh_acquirer || !same_incarnation {
                AggPeerChannel {
                    applied: transition_frontier,
                    accepted: transition_frontier,
                    events: VecDeque::new(),
                }
            } else {
                self.peer_channels.get(&peer).map_or(
                    AggPeerChannel {
                        applied: transition_frontier,
                        accepted: transition_frontier,
                        events: VecDeque::new(),
                    },
                    |channel| AggPeerChannel {
                        applied: channel.applied,
                        accepted: channel.accepted,
                        events: VecDeque::new(),
                    },
                )
            };
            channels.insert(peer, channel);
        }
        let local_frontier = if fresh_acquirer {
            transition_frontier
        } else {
            self.local_frontier
        };
        let effective = merge_input_frontier_iter(
            std::iter::once(local_frontier).chain(channels.values().map(|channel| channel.applied)),
            i64::MIN,
        );
        self.validate_frontier(transition_frontier, effective, "transition target")?;
        if fresh_acquirer && effective != transition_frontier {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' reset target channels do not form the transition cut",
                self.op_name
            )));
        }
        let last_broadcast = if target_peers.is_empty() {
            local_frontier
        } else {
            InputFrontier::default()
        };
        let topology = PreparedAggTopology {
            assignment,
            assignment_digest: transition.target.assignment_digest,
            peers: target_peers.into(),
            channels,
            local_frontier,
            last_broadcast,
            effective_frontier: effective,
        };
        let prepared_bytes = aggregate
            .accounted_state_bytes()
            .checked_add(topology.accounted_state_bytes())
            .and_then(|bytes| bytes.checked_add(payload_phase_bytes))
            .and_then(|bytes| bytes.checked_add(live_bytes))
            .ok_or_else(|| self.accounting_error())?;
        if prepared_bytes > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("aggregate '{}' prepared transition", self.op_name),
                accounted_bytes: prepared_bytes,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        self.prepared_vnode_transition = Some(PreparedSqlVnodeTransition {
            aggregate,
            topology,
        });
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn abort_vnode_transition(&mut self) {
        let Some(prepared) = self.prepared_vnode_transition.take() else {
            return;
        };
        assert!(
            self.vnode_transition_cleanup.is_none(),
            "aggregate vnode transition cleanup must finish before abort"
        );
        self.vnode_transition_cleanup = Some(SqlVnodeTransitionCleanup::Aborted(prepared));
    }

    #[cfg(feature = "cluster")]
    fn publish_vnode_transition(&mut self) {
        self.invalidate_local_aggregate_output_cache();
        let prepared = self
            .prepared_vnode_transition
            .take()
            .expect("aggregate vnode transition must be prepared before publication");
        assert!(
            self.vnode_transition_cleanup.is_none(),
            "aggregate vnode transition cleanup must finish before publication"
        );
        let QueryState::Agg(ref mut aggregate) = self.state else {
            panic!("managed vnode transition publication targeted a non-aggregate query");
        };
        let PreparedSqlVnodeTransition {
            aggregate: prepared_aggregate,
            topology,
        } = prepared;
        let retired_aggregate = aggregate.publish_prepared_vnode_transition(prepared_aggregate);
        let retired_topology = PreparedAggTopology {
            assignment: self
                .cluster_assignment
                .replace(topology.assignment)
                .expect("published aggregate topology has an installed assignment"),
            assignment_digest: self
                .cluster_assignment_digest
                .replace(topology.assignment_digest)
                .expect("published aggregate topology has an installed digest"),
            peers: std::mem::replace(&mut self.cluster_peers, topology.peers),
            channels: std::mem::replace(&mut self.peer_channels, topology.channels),
            local_frontier: self.local_frontier,
            last_broadcast: self.last_broadcast,
            effective_frontier: self.effective_frontier,
        };
        self.local_frontier = topology.local_frontier;
        self.last_broadcast = topology.last_broadcast;
        self.effective_frontier = topology.effective_frontier;
        self.remote_peer_cursor = None;
        self.queued_payload_bytes = 0;
        self.queued_event_capacity_bytes = 0;
        self.queued_remote_events = 0;
        self.vnode_transition_cleanup = Some(SqlVnodeTransitionCleanup::Published {
            aggregate: retired_aggregate,
            topology: retired_topology,
        });
    }

    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {
        match self.vnode_transition_cleanup.take() {
            Some(SqlVnodeTransitionCleanup::Aborted(prepared)) => drop(prepared),
            Some(SqlVnodeTransitionCleanup::Published {
                aggregate,
                topology,
            }) => {
                IncrementalAggState::finish_vnode_transition(aggregate);
                drop(topology);
            }
            None => {}
        }
    }

    fn force_full_vnode_capture(&mut self) {
        self.invalidate_local_aggregate_output_cache();
        if let QueryState::Agg(aggregate) = &mut self.state {
            aggregate.force_full_vnode_capture();
        }
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    #[cfg(feature = "cluster")]
    use crate::operator_graph::{ManagedVnodeRestore, ManagedVnodeTransitionMode};
    use arrow::array::{DictionaryArray, Int64Array, Int8Array, StringArray};
    use arrow::datatypes::{Field, Int8Type, Schema};

    #[test]
    fn sql_capability_classification_is_shape_aware_and_fail_closed() {
        use crate::operator::capability::{
            ClusterExecutionStatus, ManagedStateContract, OperatorStateClass,
        };

        let context = laminar_sql::create_session_context();
        let classify = |sql| classify_sql_capability(sql, &context);

        let stateless = classify("SELECT key, value * 2 FROM events");
        assert_eq!(stateless.state_class, OperatorStateClass::Stateless);
        assert_eq!(stateless.cluster_status, ClusterExecutionStatus::DdlGuarded);

        let scalar = classify("SELECT UPPER(key) FROM events");
        assert_eq!(scalar.state_class, OperatorStateClass::Stateless);
        assert_eq!(scalar.cluster_status, ClusterExecutionStatus::DdlGuarded);

        let global = classify("SELECT COUNT(*) AS n FROM events");
        assert_eq!(global.state_class, OperatorStateClass::GlobalSingleton);
        assert_eq!(global.cluster_status, ClusterExecutionStatus::DdlGuarded);
        assert_eq!(
            global.managed_state,
            Some(ManagedStateContract::SqlAggregateV1)
        );

        let keyed = classify("SELECT key, SUM(value) AS total FROM events GROUP BY key");
        assert_eq!(keyed.state_class, OperatorStateClass::VnodeKeyed);
        assert_eq!(keyed.cluster_status, ClusterExecutionStatus::DdlGuarded);
        assert_eq!(
            keyed.managed_state,
            Some(ManagedStateContract::SqlAggregateV1)
        );

        let window_keyed = classify(
            "SELECT TUMBLE(ts, INTERVAL '1' MINUTE), SUM(value) FROM events \
             GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)",
        );
        assert_eq!(window_keyed.state_class, OperatorStateClass::VnodeKeyed);
        let ClusterExecutionStatus::Rejected { reason } = window_keyed.cluster_status else {
            panic!("windowed aggregate must remain rejected")
        };
        assert!(reason.contains("timer"), "{reason}");
        assert!(reason.contains("watermark"), "{reason}");
        assert_eq!(window_keyed.managed_state, None);

        let analytic = classify("SELECT SUM(value) OVER (PARTITION BY key) AS running FROM events");
        assert_eq!(analytic.state_class, OperatorStateClass::LocalOnly);
        assert!(matches!(
            analytic.cluster_status,
            ClusterExecutionStatus::Rejected { .. }
        ));

        for ambiguous_sql in [
            "SELECT mystery(value) FROM events",
            "SELECT DISTINCT key FROM events",
            "SELECT key FROM events GROUP BY key",
            "SELECT key FROM (SELECT key FROM events) nested",
            "SELECT a.key FROM events a JOIN other b ON a.key = b.key",
            "SELECT COUNT(*) FROM (SELECT * FROM events) nested",
            "WITH nested AS (SELECT * FROM events) SELECT COUNT(*) FROM nested",
            "SELECT COUNT(*) FROM events a JOIN other b ON a.key = b.key",
            "SELECT DISTINCT COUNT(*) FROM events",
            "SELECT key FROM events ORDER BY key",
            "SELECT COUNT(*) AS n FROM events LIMIT 1",
            "SELECT key FROM events; SELECT key FROM events",
        ] {
            let ambiguous = classify(ambiguous_sql);
            assert_eq!(
                ambiguous.state_class,
                OperatorStateClass::LocalOnly,
                "{ambiguous_sql}"
            );
            assert!(
                matches!(
                    ambiguous.cluster_status,
                    ClusterExecutionStatus::Rejected { .. }
                ),
                "{ambiguous_sql}"
            );
        }

        let malformed = classify("not sql");
        assert_eq!(malformed.state_class, OperatorStateClass::LocalOnly);
        assert!(matches!(
            malformed.cluster_status,
            ClusterExecutionStatus::Rejected { .. }
        ));
    }

    #[tokio::test]
    async fn managed_aggregate_initializes_before_receiving_input() {
        let (context, batch) = context_and_batch();
        let key_group_count = KeyGroupCount::try_from(8_u16).unwrap();
        let mut operator = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
            key_group_count,
        );

        operator.initialize_managed_state().await.unwrap();

        let QueryState::Agg(ref aggregate) = operator.state else {
            panic!("expected initialized aggregate state");
        };
        assert_eq!(aggregate.key_group_count(), key_group_count);
        let empty_accounting = operator
            .managed_state_accounting()
            .expect("initialized aggregate must report managed state");
        assert!(empty_accounting.live > 0);
        assert_eq!(empty_accounting.prepared, 0);
        assert_eq!(empty_accounting.retired, 0);

        operator.process(&[vec![batch]], &[i64::MIN]).await.unwrap();
        let populated_accounting = operator.managed_state_accounting().unwrap();
        assert!(populated_accounting.live > empty_accounting.live);
        assert_eq!(populated_accounting.prepared, 0);
        assert_eq!(populated_accounting.retired, 0);
        assert!(operator.checkpoint().unwrap().is_none());
        let captured = operator
            .checkpoint_vnodes(&(0..8).collect::<Vec<_>>(), 8, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(captured.len(), 8);
        assert!(captured.iter().all(|frame| frame.state.is_some()));
    }

    #[tokio::test]
    async fn managed_aggregate_rejects_nested_checkpoint_types_at_initialization() {
        let cases = [
            (
                "nested-group",
                "SELECT make_array(key) AS keys, COUNT(*) AS count FROM events GROUP BY make_array(key)",
                "group key",
            ),
            (
                "nested-result",
                "SELECT MIN(struct(key, value)) AS min_pair FROM events",
                "emitted result",
            ),
        ];

        for (name, sql, component) in cases {
            let (context, _) = context_and_batch();
            let mut operator = SqlQueryOperator::new(name, sql, context, None, false);
            let error = operator
                .initialize_managed_state()
                .await
                .expect_err("nested aggregate checkpoint types must fail during initialization");
            assert!(matches!(&error, DbError::Unsupported(_)), "{error}");
            assert!(
                error
                    .to_string()
                    .contains(laminar_core::error_codes::SQL_UNSUPPORTED),
                "{error}"
            );
            assert!(error.to_string().contains(component), "{error}");
            assert!(matches!(operator.state, QueryState::Uninit));
        }
    }

    #[tokio::test]
    async fn local_full_state_aggregate_reuses_budgeted_final_output_on_empty_cycles() {
        let (context, batch) = context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "cached-counts",
            "SELECT key, COUNT(*) AS count FROM events GROUP BY key",
            context,
            None,
            false,
        );

        let first = operator
            .process(&[vec![batch.clone()]], &[i64::MIN])
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        let cached_bytes = operator
            .cached_local_aggregate_output
            .as_ref()
            .expect("successful final post-HAVING output must be cached")
            .retained_bytes;
        assert!(
            cached_bytes
                >= first
                    .iter()
                    .map(RecordBatch::get_array_memory_size)
                    .sum::<usize>(),
            "the cache charge must cover retained Arrow backing allocations"
        );
        let QueryState::Agg(aggregate) = &operator.state else {
            panic!("count query must use aggregate state");
        };
        assert_eq!(
            operator.managed_state_accounting().unwrap().live,
            aggregate
                .accounted_state_bytes()
                .checked_add(cached_bytes)
                .unwrap(),
            "the retained output is part of managed live state"
        );

        let empty = operator.process(&[Vec::new()], &[123]).await.unwrap();
        assert_eq!(empty.len(), first.len());
        for (original, reused) in first.iter().zip(&empty) {
            assert_eq!(original.schema(), reused.schema());
            for (original, reused) in original.columns().iter().zip(reused.columns()) {
                assert!(
                    Arc::ptr_eq(original, reused),
                    "an empty cycle must shallow-clone the cached Arrow output"
                );
            }
        }

        let updated = operator
            .process(&[vec![batch.slice(0, 1)]], &[456])
            .await
            .unwrap();
        assert_eq!(updated.len(), 1);
        assert!(
            first[0]
                .columns()
                .iter()
                .zip(updated[0].columns())
                .any(|(before, after)| !Arc::ptr_eq(before, after)),
            "a state mutation must replace the cached emission"
        );
        let updated_empty = operator.process(&[Vec::new()], &[789]).await.unwrap();
        assert!(updated[0]
            .columns()
            .iter()
            .zip(updated_empty[0].columns())
            .all(|(original, reused)| Arc::ptr_eq(original, reused)));

        let QueryState::Agg(aggregate) = &operator.state else {
            panic!("count query must retain aggregate state");
        };
        let aggregate_bytes = aggregate.accounted_state_bytes();
        let updated_cache_bytes = operator
            .cached_local_aggregate_output
            .as_ref()
            .expect("updated full-state output must replace the cache")
            .retained_bytes;
        let tight_budget = aggregate_bytes
            .checked_add(updated_cache_bytes)
            .and_then(|bytes| bytes.checked_sub(1))
            .unwrap();
        operator.set_managed_state_budget(tight_budget);
        assert!(operator.cached_local_aggregate_output.is_none());

        let tight_output = operator
            .process(&[vec![batch.slice(0, 1)]], &[1_000])
            .await
            .expect("declining the optional cache must not fail aggregate processing");
        assert_eq!(tight_output.len(), 1);
        assert!(
            operator.cached_local_aggregate_output.is_none(),
            "live aggregate plus output above the operator budget must not be cached"
        );
        let uncached_empty = operator
            .process(&[Vec::new()], &[1_001])
            .await
            .expect("an uncached empty cycle must retain explicit full-state semantics");
        assert_eq!(uncached_empty.len(), 1);
        assert!(operator.cached_local_aggregate_output.is_none());
        assert!(tight_output[0]
            .columns()
            .iter()
            .zip(uncached_empty[0].columns())
            .any(|(before, after)| !Arc::ptr_eq(before, after)));
    }

    #[tokio::test]
    async fn changelog_aggregate_having_is_rejected_at_state_startup() {
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "qualified-sums",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key HAVING SUM(value) > 0",
            context,
            None,
            true,
        );

        let error = operator
            .initialize_managed_state()
            .await
            .expect_err("changelog HAVING must fail before state becomes executable");
        assert!(
            error.to_string().contains("transition-aware HAVING"),
            "{error}"
        );
        assert!(matches!(operator.state, QueryState::Uninit));
    }

    #[tokio::test]
    async fn weighted_projection_compiled_and_cached_paths_share_one_sql_envelope() {
        let context = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new(
                laminar_core::changelog::WEIGHT_COLUMN,
                DataType::Int64,
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![1, -1, 2])),
            ],
        )
        .unwrap();
        let provider = Arc::new(laminar_sql::datafusion::LiveSourceProvider::new(schema));
        let handle = provider.handle();
        context.register_table("changes", provider).unwrap();
        handle.swap(vec![batch.clone()]);
        let sql = "SELECT value + 1 AS adjusted FROM changes WHERE id >= 2";

        let mut compiled =
            SqlQueryOperator::new("weighted-compiled", sql, context.clone(), None, false);
        compiled.lazy_init().await.unwrap();
        assert!(matches!(compiled.state, QueryState::Compiled(_)));
        let compiled_output = compiled
            .process(&[vec![batch.clone()]], &[i64::MIN])
            .await
            .unwrap();

        let mut cached =
            SqlQueryOperator::new("weighted-cached", sql, context.clone(), None, false);
        cached.lazy_init().await.unwrap();
        assert_eq!(cached.sql.matches("__weight").count(), 2);
        cached.build_and_cache_physical_plan().await.unwrap();
        assert!(matches!(cached.state, QueryState::CachedPlan(_)));
        let cached_output = cached
            .process(&[vec![batch.clone()]], &[i64::MIN])
            .await
            .unwrap();

        assert_eq!(compiled_output.len(), 1);
        assert_eq!(cached_output.len(), 1);
        assert_eq!(compiled_output[0].schema(), cached_output[0].schema());
        let weight = laminar_core::changelog::WEIGHT_COLUMN;
        let weight_field = compiled_output[0].schema().field(1).clone();
        assert_eq!(weight_field.name(), weight);
        assert!(!weight_field.is_nullable());
        for output in [&compiled_output, &cached_output] {
            let adjusted = output[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let weights = output[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(adjusted.values(), &[21, 31]);
            assert_eq!(weights.values(), &[-1, 2]);
        }

        let wildcard_sql = "SELECT id AS copy, * FROM changes WHERE id >= 2";
        let mut wildcard_compiled = SqlQueryOperator::new(
            "weighted-wildcard-compiled",
            wildcard_sql,
            context.clone(),
            None,
            false,
        );
        wildcard_compiled.lazy_init().await.unwrap();
        assert!(matches!(wildcard_compiled.state, QueryState::Compiled(_)));
        let wildcard_compiled_output = wildcard_compiled
            .process(&[vec![batch.clone()]], &[i64::MIN])
            .await
            .unwrap();

        let mut wildcard_cached = SqlQueryOperator::new(
            "weighted-wildcard-cached",
            wildcard_sql,
            context,
            None,
            false,
        );
        wildcard_cached.lazy_init().await.unwrap();
        wildcard_cached
            .build_and_cache_physical_plan()
            .await
            .unwrap();
        assert!(matches!(wildcard_cached.state, QueryState::CachedPlan(_)));
        let wildcard_cached_output = wildcard_cached
            .process(&[vec![batch]], &[i64::MIN])
            .await
            .unwrap();

        assert_eq!(wildcard_compiled_output.len(), 1);
        assert_eq!(wildcard_cached_output.len(), 1);
        assert_eq!(
            wildcard_compiled_output[0].schema(),
            wildcard_cached_output[0].schema()
        );
        assert_eq!(
            wildcard_compiled_output[0]
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["copy", "id", "value", weight]
        );
        for output in [&wildcard_compiled_output, &wildcard_cached_output] {
            let copy = output[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let weights = output[0]
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(copy.values(), &[2, 3]);
            assert_eq!(weights.values(), &[-1, 2]);
        }
    }

    #[tokio::test]
    async fn local_aggregate_coalescing_preserves_append_rows_and_state() {
        let context = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let seed = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["seed"])),
                Arc::new(Int64Array::from(vec![0])),
            ],
        )
        .unwrap();
        let table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![seed]])
                .unwrap();
        context.register_table("changes", Arc::new(table)).unwrap();

        let mut tiny_inputs = Vec::new();
        let mut expected_projected_rows = Vec::new();
        for index in 0..2_050usize {
            let key = format!("k{}", index % 7);
            let value = i64::try_from(index).unwrap() + 1;
            tiny_inputs.push(
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(StringArray::from(vec![key.as_str()])),
                        Arc::new(Int64Array::from(vec![value])),
                    ],
                )
                .unwrap(),
            );
            expected_projected_rows.push((key, value));
        }
        let combined_input =
            arrow::compute::concat_batches(&schema, tiny_inputs.as_slice()).unwrap();
        let sql = "SELECT key, COUNT(*) AS match_count, MAX(value) AS max_value \
                   FROM changes GROUP BY key";

        let mut coalesced_operator =
            SqlQueryOperator::new("coalesced", sql, context.clone(), None, false);
        coalesced_operator.lazy_init().await.unwrap();
        let projected = coalesced_operator
            .pre_aggregate(&tiny_inputs)
            .await
            .unwrap();
        let QueryState::Agg(aggregate) = &coalesced_operator.state else {
            panic!("coalesced operator must be initialized");
        };
        assert!(aggregate.certifies_local_input_coalescing());
        let projected_schema = projected[0].schema();
        let coalesced = coalesced_operator
            .prepare_local_aggregate_batches(projected)
            .unwrap();
        assert_eq!(
            coalesced
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [1_024, 1_024, 2],
            "the actual aggregate apply input is bounded by the local row target"
        );
        assert!(coalesced.iter().all(|batch| {
            batch.schema().as_ref() == projected_schema.as_ref()
                && batch.num_rows() <= LOCAL_AGG_COALESCE_MAX_BATCH_ROWS
                && laminar_core::shuffle::logical_batch_bytes(batch).unwrap()
                    <= LOCAL_AGG_COALESCE_TARGET_BATCH_BYTES
        }));
        let actual_projected_rows = coalesced
            .iter()
            .flat_map(|batch| {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| (keys.value(row).to_owned(), values.value(row)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual_projected_rows, expected_projected_rows);

        let tiny_ports = vec![tiny_inputs];
        let coalesced_output = coalesced_operator
            .process(&tiny_ports, &[777])
            .await
            .unwrap();
        let mut single_batch_operator =
            SqlQueryOperator::new("single-batch", sql, context, None, false);
        let single_batch_ports = vec![vec![combined_input]];
        let single_batch_output = single_batch_operator
            .process(&single_batch_ports, &[777])
            .await
            .unwrap();

        let output_rows = |batches: &[RecordBatch]| {
            let mut rows = std::collections::BTreeMap::new();
            for batch in batches {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let counts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let maxima = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    rows.insert(
                        keys.value(row).to_owned(),
                        (counts.value(row), maxima.value(row)),
                    );
                }
            }
            rows
        };
        assert_eq!(
            coalesced_output[0].schema(),
            single_batch_output[0].schema()
        );
        assert_eq!(
            output_rows(&coalesced_output),
            output_rows(&single_batch_output)
        );

        let QueryState::Agg(coalesced_state) = &coalesced_operator.state else {
            panic!("coalesced operator must retain aggregate state");
        };
        let QueryState::Agg(single_batch_state) = &single_batch_operator.state else {
            panic!("single-batch operator must retain aggregate state");
        };
        assert_eq!(coalesced_state.logical_group_count_for_test(), 7);
        assert_eq!(
            coalesced_state.working_set_snapshot_for_test(),
            single_batch_state.working_set_snapshot_for_test()
        );
    }

    #[test]
    fn local_aggregate_coalescing_preserves_dictionary_batch_boundaries() {
        let batches = (0..130)
            .map(|index| {
                let values: arrow::array::ArrayRef =
                    Arc::new(StringArray::from(vec![format!("dictionary-{index}")]));
                let dictionary =
                    DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), values).unwrap();
                RecordBatch::try_from_iter(vec![(
                    "dictionary_value",
                    Arc::new(dictionary) as arrow::array::ArrayRef,
                )])
                .unwrap()
            })
            .collect::<Vec<_>>();

        let preserved = coalesce_local_aggregate_batches("dictionary", batches).unwrap();
        assert_eq!(preserved.len(), 130);
        for (index, batch) in preserved.iter().enumerate() {
            let dictionary = batch
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<Int8Type>>()
                .unwrap();
            let values = dictionary
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(values.value(0), format!("dictionary-{index}"));
        }
    }

    #[tokio::test]
    async fn local_aggregate_coalescing_preserves_weighted_prefix_rejection() {
        let context = laminar_sql::create_session_context();
        let weight = laminar_core::changelog::WEIGHT_COLUMN;
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
            Field::new(weight, DataType::Int64, false),
        ]));
        let seed = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["seed"])),
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![seed]])
                .unwrap();
        context.register_table("changes", Arc::new(table)).unwrap();

        let weighted_batch = |row_weight| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec!["absent"])),
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![row_weight])),
                ],
            )
            .unwrap()
        };
        let mut operator = SqlQueryOperator::new(
            "weighted-prefix",
            "SELECT key, COUNT(*) AS count FROM changes GROUP BY key",
            context,
            None,
            false,
        );
        operator.lazy_init().await.unwrap();
        let QueryState::Agg(aggregate) = &operator.state else {
            panic!("weighted operator must be initialized");
        };
        assert!(!aggregate.certifies_local_input_coalescing());

        let weighted_inputs = vec![weighted_batch(-1), weighted_batch(1)];
        let projected = operator.pre_aggregate(&weighted_inputs).await.unwrap();
        let preserved = operator.prepare_local_aggregate_batches(projected).unwrap();
        assert_eq!(preserved.len(), 2);
        assert_eq!(
            preserved
                .iter()
                .map(|batch| {
                    batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0)
                })
                .collect::<Vec<_>>(),
            [-1, 1]
        );

        let inputs = vec![weighted_inputs];
        let error = operator.process(&inputs, &[777]).await.unwrap_err();
        assert!(
            error.to_string().contains("input weight became negative"),
            "weighted batches must retain their prefix validation boundary: {error}"
        );
    }

    #[tokio::test]
    async fn local_aggregate_coalescing_does_not_resegment_sum() {
        let (context, batch) = context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "sum-boundaries",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );
        operator.lazy_init().await.unwrap();
        let QueryState::Agg(aggregate) = &operator.state else {
            panic!("SUM operator must be initialized");
        };
        assert!(!aggregate.certifies_local_input_coalescing());

        let inputs = vec![batch.slice(0, 1), batch.slice(1, 1)];
        let projected = operator.pre_aggregate(&inputs).await.unwrap();
        assert_eq!(projected.len(), 2);
        let preserved = operator.prepare_local_aggregate_batches(projected).unwrap();
        assert_eq!(
            preserved.len(),
            2,
            "SUM keeps its original overflow and floating-point reduction boundaries"
        );
    }

    pub(super) fn context_and_batch() -> (SessionContext, RecordBatch) {
        let context = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let seed = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["seed"])),
                Arc::new(Int64Array::from(vec![0])),
            ],
        )
        .unwrap();
        let table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![seed]])
                .unwrap();
        context.register_table("events", Arc::new(table)).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        (context, batch)
    }

    fn unaligned_aggregate_archive_transport(bytes: &[u8]) -> bytes::Bytes {
        let mut transport = vec![0_u8; bytes.len() + AGG_CHECKPOINT_ARCHIVE_ALIGNMENT];
        let base = transport.as_ptr() as usize;
        let offset = (0..AGG_CHECKPOINT_ARCHIVE_ALIGNMENT)
            .find(|offset| (base + offset) % AGG_CHECKPOINT_ARCHIVE_ALIGNMENT != 0)
            .expect("an aggregate archive transport offset must be unaligned");
        transport[offset..offset + bytes.len()].copy_from_slice(bytes);
        let bytes = bytes::Bytes::from(transport).slice(offset..offset + bytes.len());
        assert_ne!(
            bytes
                .as_ptr()
                .align_offset(AGG_CHECKPOINT_ARCHIVE_ALIGNMENT),
            0
        );
        bytes
    }

    #[cfg(feature = "cluster")]
    async fn cluster_scope(owners: [u64; 8]) -> ClusterShuffleConfig {
        use std::time::Duration;

        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::cluster::control::LeaseDeadline;
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use laminar_core::state::VnodeRegistry;

        let registry = Arc::new(VnodeRegistry::new(8));
        registry.set_assignment(Arc::from(owners.map(NodeId)));
        let incarnation = uuid::Uuid::from_u128(1);
        let receiver = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), incarnation)
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(1, incarnation));
        let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
        receiver
            .install_process_lease_deadline(Arc::clone(&deadline))
            .unwrap();
        sender.install_process_lease_deadline(deadline).unwrap();
        let participants = owners
            .iter()
            .copied()
            .filter(|node| *node != 0)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|node_id| CheckpointParticipant {
                node_id,
                boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
            })
            .collect();
        let fence = CheckpointAssignmentFence::from_owner_map(
            registry.assignment_version(),
            &owners,
            participants,
        )
        .unwrap();
        sender.install_assignment_fence(&fence, &owners).unwrap();
        receiver.install_assignment_fence(&fence, &owners).unwrap();
        ClusterShuffleConfig {
            registry,
            sender,
            receiver,
            self_id: NodeId(1),
        }
    }

    #[cfg(feature = "cluster")]
    fn portable_aggregate_whole_frame(
        predecessor: &laminar_core::checkpoint::CheckpointAssignmentFence,
        donor: u64,
        frontier: InputFrontier,
    ) -> Vec<u8> {
        let channels = predecessor
            .participant_ids()
            .into_iter()
            .filter(|peer| *peer != donor)
            .map(|peer| AggCheckpointChannel {
                peer,
                applied: frontier.into(),
                events: Vec::new(),
            })
            .collect();
        rkyv::to_bytes::<rkyv::rancor::Error>(&AggOpCheckpoint {
            version: AGG_OP_CHECKPOINT_VERSION,
            assignment_version: predecessor.assignment_version,
            owner_map_digest: predecessor.assignment_digest,
            self_id: donor,
            recovery_gen: 0,
            local_frontier: frontier.into(),
            effective_frontier: frontier.into(),
            remote_peer_cursor: None,
            channels,
        })
        .unwrap()
        .to_vec()
    }

    #[cfg(feature = "cluster")]
    fn projected_batch_for_vnode(
        operator: &SqlQueryOperator,
        vnode: u32,
        value: i64,
    ) -> (String, RecordBatch) {
        let QueryState::Agg(aggregate) = &operator.state else {
            panic!("aggregate must be initialized");
        };
        let projection = aggregate.compiled_projection().unwrap();
        for index in 0..1_000 {
            let key = format!("K{index}");
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ]));
            let input = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec![key.as_str()])),
                    Arc::new(Int64Array::from(vec![value])),
                ],
            )
            .unwrap();
            let projected = projection.evaluate(&input).unwrap();
            let routed = hash_rows_to_vnodes(
                &projected,
                aggregate.num_group_cols(),
                u32::from(operator.key_group_count),
            )
            .unwrap();
            if routed == [vnode] {
                return (key, projected);
            }
        }
        panic!("no test key hashes to vnode {vnode}");
    }

    #[cfg(feature = "cluster")]
    fn projected_batch_for_key(operator: &SqlQueryOperator, key: &str, value: i64) -> RecordBatch {
        let QueryState::Agg(aggregate) = &operator.state else {
            panic!("aggregate must be initialized");
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let input = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![key])),
                Arc::new(Int64Array::from(vec![value])),
            ],
        )
        .unwrap();
        aggregate
            .compiled_projection()
            .unwrap()
            .evaluate(&input)
            .unwrap()
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn checkpointed_remote_frontiers_compare_in_receiver_domain() {
        let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
            KeyGroupCount::try_from(8_u16).unwrap(),
        );
        operator.initialize_managed_state().await.unwrap();
        operator.attach_cluster_shuffle(scope.clone());
        operator.effective_frontier = InputFrontier {
            watermark: Some(500),
            idle: false,
        };
        let idle = InputFrontier {
            watermark: Some(100),
            idle: true,
        };
        let channel = operator.peer_channels.get_mut(&2).unwrap();
        channel.applied = idle;
        channel.accepted = idle;
        let assignment = scope.registry.assignment_version();
        let recovery = scope.receiver.recovery_gen();
        let active = |watermark| InputFrontier {
            watermark: Some(watermark),
            idle: false,
        };

        operator
            .stage_checkpointed_shuffle_frontier("sum", 2, active(100), assignment, recovery)
            .unwrap();
        assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(500));
        operator
            .stage_checkpointed_shuffle_frontier("sum", 2, active(150), assignment, recovery)
            .unwrap();
        assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(500));
        operator
            .stage_checkpointed_shuffle_frontier("sum", 2, active(550), assignment, recovery)
            .unwrap();
        assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(550));
        assert!(operator
            .stage_checkpointed_shuffle_frontier(
                "sum",
                2,
                InputFrontier {
                    watermark: None,
                    idle: false,
                },
                assignment,
                recovery,
            )
            .is_err());
        assert!(operator
            .stage_checkpointed_shuffle_frontier("sum", 2, active(525), assignment, recovery,)
            .is_err());
        assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(550));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn restored_frontier_bootstrap_precedes_live_source_frontier() {
        let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
            KeyGroupCount::try_from(8_u16).unwrap(),
        );
        operator.initialize_managed_state().await.unwrap();
        let (key, local) = projected_batch_for_vnode(&operator, 0, 42);
        let buffered = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![key.as_str()])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap();
        operator.attach_cluster_shuffle(scope.clone());
        let restored = InputFrontier {
            watermark: Some(100),
            idle: false,
        };
        let live = InputFrontier {
            watermark: Some(1_000),
            idle: false,
        };
        operator.local_frontier = restored;
        operator.effective_frontier = restored;
        operator.last_broadcast = InputFrontier::default();
        let channel = operator.peer_channels.get_mut(&2).unwrap();
        channel.applied = restored;
        channel.accepted = restored;

        assert!(!operator.wants_input());
        let assignment = scope.registry.versioned_snapshot();
        let bootstrap = operator.cluster_cycle_local_frontier(live, false).unwrap();
        assert_eq!(bootstrap, restored);
        let plan = operator
            .plan_cluster_batches(Vec::new(), bootstrap, &scope, &assignment, &[2])
            .unwrap();
        assert!(matches!(
            plan.outbound.as_slice(),
            [(
                2,
                ShuffleMessage::Frontier {
                    watermark: Some(100),
                    idle: false,
                    ..
                }
            )]
        ));
        operator.process_cluster(&[Vec::new()], live).await.unwrap();
        let mut pending = operator.pending_cluster_input.take().unwrap();
        assert_eq!(pending.local_frontier, restored);
        assert!(pending.local_batches.is_empty());
        pending.send.take().unwrap().abort();

        // Simulate completion of the bootstrap send. The graph may now release its retained row,
        // and the ordinary node-local frontier is used without being globally frozen.
        operator.last_broadcast = restored;
        assert!(operator.wants_input());
        let admitted = operator.cluster_cycle_local_frontier(live, true).unwrap();
        assert_eq!(admitted, live);
        let plan = operator
            .plan_cluster_batches(vec![local], admitted, &scope, &assignment, &[2])
            .unwrap();
        assert_eq!(plan.local_batches.len(), 1);
        assert_eq!(plan.local_frontier, live);
        operator
            .process_cluster(&[vec![buffered]], live)
            .await
            .unwrap();
        let mut pending = operator.pending_cluster_input.take().unwrap();
        assert_eq!(pending.local_frontier, live);
        assert_eq!(pending.local_batches.len(), 1);
        pending.send.take().unwrap().abort();
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn pending_send_drains_remote_sum_before_publishing_local_cut() {
        use std::time::Duration;

        let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
            KeyGroupCount::try_from(8_u16).unwrap(),
        );
        operator.initialize_managed_state().await.unwrap();
        let (key, local) = projected_batch_for_vnode(&operator, 0, 8);
        let remote = projected_batch_for_key(&operator, &key, 34);
        let (_, outbound_batch) = projected_batch_for_vnode(&operator, 1, 1);
        operator.attach_cluster_shuffle(scope.clone());
        let frontier = InputFrontier {
            watermark: Some(100),
            idle: false,
        };
        let assignment = scope.registry.versioned_snapshot();
        let plan = operator
            .plan_cluster_batches(
                vec![local, outbound_batch],
                frontier,
                &scope,
                &assignment,
                &[2],
            )
            .unwrap();
        assert_eq!(plan.local_batches.len(), 1);
        let accounted_bytes = operator.cluster_input_plan_bytes(&plan).unwrap();
        let AggClusterInputPlan {
            local_batches,
            outbound,
            local_frontier,
            effective_frontier: _,
        } = plan;
        let (release, held) = tokio::sync::oneshot::channel();
        let (completion_tx, completion) = tokio::sync::oneshot::channel();
        let send = tokio::spawn(async move {
            let _ = held.await;
            drop(outbound);
            let _ = completion_tx.send((Ok(()), None));
        });
        operator.pending_cluster_input = Some(PendingAggClusterInput {
            local_batches,
            outbound: None,
            local_frontier,
            send: Some(send),
            completion: Some(completion),
            accounted_bytes,
        });
        let version = assignment.version();
        let recovery = scope.receiver.recovery_gen();
        operator
            .stage_checkpointed_shuffle(
                "sum",
                crate::operator::RetainedBatch::restored_channel(
                    remote,
                    2,
                    version,
                    recovery,
                    Arc::from([0_u32]),
                ),
                i64::MIN,
            )
            .unwrap();
        operator
            .stage_checkpointed_shuffle_frontier("sum", 2, frontier, version, recovery)
            .unwrap();

        let remote_output = tokio::time::timeout(
            Duration::from_millis(50),
            operator.process_cluster(&[Vec::new()], InputFrontier::default()),
        )
        .await
        .expect("held send blocked remote replay")
        .unwrap();
        assert_eq!(
            remote_output
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        assert_eq!(operator.queued_remote_events, 1);
        assert_eq!(operator.local_frontier, InputFrontier::default());
        assert!(!operator.wants_input());
        assert!(operator.checkpoint_drain_pending());
        assert!(operator.checkpoint_vnodes(&[0], 8, u64::MAX).is_err());

        release.send(()).unwrap();
        let output = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let output = operator
                    .process_cluster(&[Vec::new()], InputFrontier::default())
                    .await
                    .unwrap();
                if operator.pending_cluster_input.is_none() {
                    break output;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let total = output[0]
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(total.value(0), 42);
        assert!(operator.pending_cluster_input.is_none());
        assert_eq!(operator.queued_remote_events, 0);
        assert_eq!(operator.local_frontier, frontier);
        assert_eq!(operator.effective_frontier, frontier);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn zero_admission_retry_has_no_runnable_spin() {
        let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
            KeyGroupCount::try_from(8_u16).unwrap(),
        );
        operator.initialize_managed_state().await.unwrap();
        operator.attach_cluster_shuffle(scope);
        let retry_plan = vec![(
            2,
            ShuffleMessage::Frontier {
                stage: "sum".to_string(),
                watermark: None,
                idle: false,
            },
        )];
        let (completion_tx, completion) = tokio::sync::oneshot::channel();
        assert!(completion_tx
            .send((
                Err(DbError::ShuffleNotReady("injected zero admission".into())),
                Some(retry_plan),
            ))
            .is_ok());
        let send = tokio::spawn(async {});
        operator.pending_cluster_input = Some(PendingAggClusterInput {
            local_batches: Vec::new(),
            outbound: None,
            local_frontier: InputFrontier::default(),
            send: Some(send),
            completion: Some(completion),
            accounted_bytes: 0,
        });
        assert!(!operator.deferred_work_is_runnable());
        operator
            .process_cluster(&[Vec::new()], InputFrontier::default())
            .await
            .unwrap();
        let pending = operator.pending_cluster_input.as_ref().unwrap();
        assert!(pending.send.is_some());
        assert!(pending.outbound.is_none());
        assert!(!operator.deferred_work_is_runnable());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn ordered_channel_checkpoint_roundtrips_under_budget() {
        let scope = cluster_scope([1, 2, 2, 2, 2, 2, 2, 2]).await;
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
            KeyGroupCount::try_from(8_u16).unwrap(),
        );
        operator.initialize_managed_state().await.unwrap();
        let (_, remote) = projected_batch_for_vnode(&operator, 0, 42);
        let remote_schema = remote.schema();
        let remote = arrow::compute::concat_batches(&remote_schema, &vec![remote; 512]).unwrap();
        operator.attach_cluster_shuffle(scope.clone());
        let version = scope.registry.assignment_version();
        let recovery = scope.receiver.recovery_gen();
        operator
            .stage_checkpointed_shuffle(
                "sum",
                crate::operator::RetainedBatch::restored_channel(
                    remote,
                    2,
                    version,
                    recovery,
                    Arc::from([0_u32]),
                ),
                i64::MIN,
            )
            .unwrap();
        let frontier = InputFrontier {
            watermark: Some(100),
            idle: false,
        };
        operator
            .stage_checkpointed_shuffle_frontier("sum", 2, frontier, version, recovery)
            .unwrap();
        assert!(operator.checkpoint_capture(0).is_err());
        let capture = operator
            .checkpoint_capture(1 << 20)
            .unwrap()
            .expect("cluster aggregate always captures its channel cut");
        let mut staged = capture.retained_bytes();
        let encoded = capture.materialize(&mut staged, 1 << 20).unwrap();

        let assert_pristine = |operator: &SqlQueryOperator| {
            assert!(!operator.whole_restore_applied);
            assert_eq!(operator.local_frontier, InputFrontier::default());
            assert_eq!(operator.last_broadcast, InputFrontier::default());
            assert_eq!(operator.effective_frontier, InputFrontier::default());
            assert!(operator.remote_peer_cursor.is_none());
            assert_eq!(operator.queued_payload_bytes, 0);
            assert_eq!(operator.queued_event_capacity_bytes, 0);
            assert_eq!(operator.queued_remote_events, 0);
            assert!(operator.peer_channels.values().all(|channel| {
                channel.applied == InputFrontier::default()
                    && channel.accepted == InputFrontier::default()
                    && channel.events.is_empty()
            }));
        };

        let published = InputFrontier {
            watermark: Some(100),
            idle: false,
        };
        let revival = InputFrontier {
            watermark: Some(50),
            idle: false,
        };
        let mut malformed =
            rkyv::from_bytes::<AggOpCheckpoint, rkyv::rancor::Error>(encoded.as_ref()).unwrap();
        malformed.local_frontier = published.into();
        malformed.effective_frontier = published.into();
        let malformed_channel = malformed.channels.first_mut().unwrap();
        malformed_channel.applied = AggCheckpointFrontier {
            watermark: Some(0),
            idle: true,
        };
        malformed_channel.events.swap(0, 1);
        let AggCheckpointEvent::Frontier {
            frontier: malformed_revival,
            ..
        } = &mut malformed_channel.events[0]
        else {
            panic!("expected queued frontier before malformed data");
        };
        *malformed_revival = revival.into();
        let malformed = rkyv::to_bytes::<rkyv::rancor::Error>(&malformed).unwrap();
        let (malformed_context, _) = context_and_batch();
        let mut malformed_restore = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            malformed_context,
            None,
            false,
            KeyGroupCount::try_from(8_u16).unwrap(),
        );
        malformed_restore.initialize_managed_state().await.unwrap();
        malformed_restore.attach_cluster_shuffle(scope.clone());
        let pristine_bytes = malformed_restore.checked_live_state_bytes().unwrap();
        assert!(matches!(
            malformed_restore.restore(OperatorCheckpoint {
                data: malformed.to_vec()
            }),
            Err(DbError::Checkpoint(_))
        ));
        assert_pristine(&malformed_restore);
        assert_eq!(
            malformed_restore.checked_live_state_bytes().unwrap(),
            pristine_bytes
        );

        let (restored_context, _) = context_and_batch();
        let mut restored = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            restored_context,
            None,
            false,
            KeyGroupCount::try_from(8_u16).unwrap(),
        );
        restored.initialize_managed_state().await.unwrap();
        restored.attach_cluster_shuffle(scope.clone());
        restored
            .restore(OperatorCheckpoint {
                data: encoded.to_vec(),
            })
            .unwrap();
        assert_eq!(restored.queued_remote_events, 2);
        assert!(restored.checkpoint_vnodes(&[0], 8, u64::MAX).is_ok());

        let decoded_accounted = restored.checked_live_state_bytes().unwrap();
        assert!(decoded_accounted > encoded.len());
        let decoded_budget = decoded_accounted - 1;
        assert!(decoded_budget >= encoded.len());
        let (limited_context, _) = context_and_batch();
        let mut limited = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            limited_context,
            None,
            false,
            KeyGroupCount::try_from(8_u16).unwrap(),
        );
        limited.initialize_managed_state().await.unwrap();
        limited.attach_cluster_shuffle(scope);
        let pristine_bytes = limited.checked_live_state_bytes().unwrap();
        limited.set_managed_state_budget(decoded_budget);
        assert!(matches!(
            limited.restore(OperatorCheckpoint {
                data: encoded.to_vec()
            }),
            Err(DbError::ManagedStateBudgetExceeded { .. })
        ));
        assert_pristine(&limited);
        assert_eq!(limited.checked_live_state_bytes().unwrap(), pristine_bytes);

        let output = restored
            .process_cluster(&[Vec::new()], InputFrontier::default())
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        restored
            .process_cluster(&[Vec::new()], InputFrontier::default())
            .await
            .unwrap();
        assert_eq!(restored.peer_channels[&2].applied, frontier);
        assert_eq!(restored.queued_remote_events, 0);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aggregate_topology_transition_is_atomic_and_accounts_retired_channels() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

        let predecessor_owners = [1, 2, 1, 1, 1, 1, 1, 1];
        let target_owners = [1, 3, 1, 1, 1, 1, 1, 1];
        let scope = cluster_scope(predecessor_owners).await;
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
            KeyGroupCount::try_from(8_u16).unwrap(),
        );
        operator.initialize_managed_state().await.unwrap();
        operator.attach_cluster_shuffle(scope.clone());
        let predecessor_version = scope.registry.assignment_version();
        let participant = |node_id| CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
        };
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            predecessor_version,
            &predecessor_owners,
            vec![participant(1), participant(2)],
        )
        .unwrap();
        let target = CheckpointAssignmentFence::from_owner_map(
            predecessor_version + 1,
            &target_owners,
            vec![participant(1), participant(3)],
        )
        .unwrap();

        let old_channel = operator.peer_channels.get_mut(&2).unwrap();
        old_channel.events.reserve(8);
        let retained_event_capacity = old_channel.events.capacity() * AGG_REMOTE_EVENT_CHARGE;
        operator.queued_event_capacity_bytes = retained_event_capacity;
        let pristine = operator.managed_state_accounting().unwrap();

        scope.registry.set_assignment_and_version(
            Arc::from(target_owners.map(NodeId)),
            target.assignment_version,
        );
        scope
            .sender
            .install_assignment_fence(&target, &target_owners)
            .unwrap();
        scope
            .receiver
            .install_assignment_fence(&target, &target_owners)
            .unwrap();
        let revoked = rustc_hash::FxHashSet::default();

        let mut wrong_digest = target.clone();
        wrong_digest.assignment_digest[0] ^= 1;
        let mut wrong_incarnation = target.clone();
        wrong_incarnation
            .participants
            .iter_mut()
            .find(|participant| participant.node_id == 1)
            .unwrap()
            .boot_incarnation = uuid::Uuid::from_u128(11);
        for invalid in [&wrong_digest, &wrong_incarnation] {
            assert!(operator
                .prepare_vnode_transition(ManagedVnodeTransition {
                    predecessor: &predecessor,
                    target: invalid,
                    revoked: &revoked,
                    restores: &[],
                    whole_restores: &[],
                    mode: ManagedVnodeTransitionMode::Live,
                })
                .is_err());
            assert!(operator.prepared_vnode_transition.is_none());
            assert!(operator.vnode_transition_cleanup.is_none());
            assert_eq!(
                operator.cluster_assignment.as_ref().unwrap().version(),
                predecessor_version
            );
            assert_eq!(operator.cluster_peers.as_ref(), &[2]);
            assert_eq!(
                operator.peer_channels[&2].events.capacity() * AGG_REMOTE_EVENT_CHARGE,
                retained_event_capacity
            );
            assert_eq!(operator.managed_state_accounting().unwrap(), pristine);
        }

        operator
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target,
                revoked: &revoked,
                restores: &[],
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        operator.publish_vnode_transition();
        assert_eq!(
            operator.cluster_assignment.as_ref().unwrap().version(),
            target.assignment_version
        );
        assert_eq!(
            operator.cluster_assignment_digest,
            Some(target.assignment_digest)
        );
        assert_eq!(operator.cluster_peers.as_ref(), &[3]);
        assert!(operator.peer_channels.contains_key(&3));
        assert!(!operator.peer_channels.contains_key(&2));
        assert_eq!(operator.queued_event_capacity_bytes, 0);

        let SqlVnodeTransitionCleanup::Published {
            aggregate,
            topology,
        } = operator.vnode_transition_cleanup.as_ref().unwrap()
        else {
            panic!("aggregate transition must retain its displaced topology");
        };
        let topology_base = topology
            .assignment
            .owners()
            .len()
            .saturating_mul(std::mem::size_of::<NodeId>() + std::mem::size_of::<u64>())
            .saturating_add(
                topology
                    .peers
                    .len()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(topology.channels.len().saturating_mul(
                std::mem::size_of::<(u64, AggPeerChannel)>() + AGG_PEER_CHANNEL_ENTRY_CHARGE,
            ));
        assert_eq!(
            topology.accounted_state_bytes(),
            topology_base + retained_event_capacity
        );
        assert_eq!(
            operator.managed_state_accounting().unwrap().retired,
            aggregate.accounted_state_bytes() + topology.accounted_state_bytes()
        );
        operator.finish_vnode_transition();
        assert!(operator.vnode_transition_cleanup.is_none());
        assert_eq!(operator.managed_state_accounting().unwrap().retired, 0);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aggregate_checkpoint_bootstrap_requires_every_whole_donor_and_installs_common_cut() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

        let predecessor_owners = [2, 3, 2, 3, 2, 3, 2, 3];
        let target_owners = [1; 8];
        // The bootstrap process was not a predecessor owner, so attach its transport directly to
        // the recovery target while retaining the independent predecessor fence below.
        let scope = cluster_scope(target_owners).await;
        let (context, batch) = context_and_batch();
        let key_groups = KeyGroupCount::try_from(8_u16).unwrap();
        let participant = |node_id| CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
        };
        let predecessor_version = scope.registry.assignment_version();
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            predecessor_version,
            &predecessor_owners,
            vec![participant(2), participant(3)],
        )
        .unwrap();
        let target = CheckpointAssignmentFence::from_owner_map(
            predecessor_version + 1,
            &target_owners,
            vec![participant(1)],
        )
        .unwrap();
        scope.registry.set_assignment_and_version(
            Arc::from(target_owners.map(NodeId)),
            target.assignment_version,
        );
        scope
            .sender
            .install_assignment_fence(&target, &target_owners)
            .unwrap();
        scope
            .receiver
            .install_assignment_fence(&target, &target_owners)
            .unwrap();
        assert_eq!(scope.receiver.recovery_gen(), 0);

        let mut donors = Vec::new();
        let frontier = InputFrontier {
            watermark: Some(777),
            idle: false,
        };
        for donor in [2, 3] {
            let mut frame = rkyv::from_bytes::<AggOpCheckpoint, rkyv::rancor::Error>(
                &portable_aggregate_whole_frame(&predecessor, donor, frontier),
            )
            .unwrap();
            // A fresh acquirer can legitimately have missed owner-only recovery rounds. The
            // donor's portable whole frame imports an empty, predecessor-bound channel cut, not
            // live transport-generation state, so a newer donor generation remains admissible.
            frame.recovery_gen = 7;
            donors.push((
                donor,
                rkyv::to_bytes::<rkyv::rancor::Error>(&frame)
                    .unwrap()
                    .to_vec(),
            ));
        }
        let whole_restores = donors
            .iter()
            .map(
                |(participant_id, state)| crate::operator_graph::ManagedWholeRestore {
                    participant_id: *participant_id,
                    state,
                },
            )
            .collect::<Vec<_>>();

        let mut donor_state = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT SUM(value) AS total FROM events",
            context.clone(),
            None,
            false,
            key_groups,
        );
        donor_state.initialize_managed_state().await.unwrap();
        donor_state.process(&[vec![batch]], &[100]).await.unwrap();
        let vnode_frames = donor_state
            .checkpoint_vnodes(&[0], 8, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|frame| {
                let capture = frame.state.unwrap();
                let mut staged = capture.retained_bytes();
                let state = capture.materialize(&mut staged, u64::MAX).unwrap();
                (frame.vnode, state)
            })
            .collect::<Vec<_>>();
        let restores = vnode_frames
            .iter()
            .map(|(vnode, state)| ManagedVnodeRestore {
                participant_id: predecessor_owners[*vnode as usize],
                vnode: *vnode,
                state,
            })
            .collect::<Vec<_>>();

        let mut target_operator = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT SUM(value) AS total FROM events",
            context,
            None,
            false,
            key_groups,
        );
        target_operator.initialize_managed_state().await.unwrap();
        target_operator.attach_cluster_shuffle(scope);
        let revoked = rustc_hash::FxHashSet::default();
        let predecessor_owner_nodes = predecessor_owners.map(NodeId);
        let transition = || ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target,
            revoked: &revoked,
            restores: &restores,
            whole_restores: &whole_restores,
            mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
                predecessor_owners: &predecessor_owner_nodes,
            },
        };

        let missing = ManagedVnodeTransition {
            whole_restores: &whole_restores[..1],
            ..transition()
        };
        assert!(target_operator.prepare_vnode_transition(missing).is_err());
        assert!(target_operator.prepared_vnode_transition.is_none());

        let mut queued_donors = donors.clone();
        let mut queued =
            rkyv::from_bytes::<AggOpCheckpoint, rkyv::rancor::Error>(&queued_donors[0].1).unwrap();
        queued.channels[0]
            .events
            .push(AggCheckpointEvent::Frontier {
                recovery_gen: 0,
                frontier: frontier.into(),
            });
        queued_donors[0].1 = rkyv::to_bytes::<rkyv::rancor::Error>(&queued)
            .unwrap()
            .to_vec();
        let queued_whole = queued_donors
            .iter()
            .map(
                |(participant_id, state)| crate::operator_graph::ManagedWholeRestore {
                    participant_id: *participant_id,
                    state,
                },
            )
            .collect::<Vec<_>>();
        let queued_transition = ManagedVnodeTransition {
            whole_restores: &queued_whole,
            ..transition()
        };
        assert!(target_operator
            .prepare_vnode_transition(queued_transition)
            .is_err());
        assert!(target_operator.prepared_vnode_transition.is_none());

        let disagreeing_donors = [
            (2, portable_aggregate_whole_frame(&predecessor, 2, frontier)),
            (
                3,
                portable_aggregate_whole_frame(
                    &predecessor,
                    3,
                    InputFrontier {
                        watermark: Some(778),
                        idle: false,
                    },
                ),
            ),
        ];
        let disagreeing_whole = disagreeing_donors
            .iter()
            .map(
                |(participant_id, state)| crate::operator_graph::ManagedWholeRestore {
                    participant_id: *participant_id,
                    state,
                },
            )
            .collect::<Vec<_>>();
        let disagreeing_transition = ManagedVnodeTransition {
            whole_restores: &disagreeing_whole,
            ..transition()
        };
        assert!(target_operator
            .prepare_vnode_transition(disagreeing_transition)
            .is_err());
        assert!(target_operator.prepared_vnode_transition.is_none());

        let unaligned_donors = donors
            .iter()
            .map(|(participant_id, state)| {
                (
                    *participant_id,
                    unaligned_aggregate_archive_transport(state),
                )
            })
            .collect::<Vec<_>>();
        let unaligned_whole = unaligned_donors
            .iter()
            .map(
                |(participant_id, state)| crate::operator_graph::ManagedWholeRestore {
                    participant_id: *participant_id,
                    state,
                },
            )
            .collect::<Vec<_>>();
        let raw_payload = restores
            .iter()
            .map(|restore| restore.state.len())
            .chain(unaligned_whole.iter().map(|restore| restore.state.len()))
            .sum::<usize>();
        let alignment_copy = restores
            .iter()
            .map(|restore| aggregate_checkpoint_alignment_copy_bytes(restore.state))
            .chain(
                unaligned_whole
                    .iter()
                    .map(|restore| aggregate_checkpoint_alignment_copy_bytes(restore.state)),
            )
            .max()
            .unwrap();
        let roster_scratch = aggregate_transition_roster_scratch_bytes(
            predecessor.participants.len(),
            restores.len(),
            restores.len(),
        )
        .unwrap();
        let payload_peak = raw_payload + alignment_copy + roster_scratch;
        target_operator.set_managed_state_budget(payload_peak - 1);
        let unaligned_transition = ManagedVnodeTransition {
            whole_restores: &unaligned_whole,
            ..transition()
        };
        assert!(matches!(
            target_operator.prepare_vnode_transition(unaligned_transition),
            Err(DbError::ManagedStateBudgetExceeded {
                accounted_bytes,
                limit_bytes,
                ..
            }) if accounted_bytes == payload_peak && limit_bytes == payload_peak - 1
        ));
        assert!(target_operator.prepared_vnode_transition.is_none());

        let QueryState::Agg(aggregate) = &target_operator.state else {
            panic!("expected aggregate transition target");
        };
        let profile = aggregate.vnode_archive_restore_profile();
        let restore_preflights = restores
            .iter()
            .map(|restore| {
                with_aligned_aggregate_checkpoint_bytes(restore.state, |state| {
                    profile
                        .preflight(state, format_args!("transition decode-bound test"))
                        .map(|archive| archive.restore_preflight())
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(restore_preflights
            .iter()
            .any(|preflight| preflight.group_count() != 0));
        let replacement_upper = restore_preflights
            .iter()
            .map(|preflight| preflight.final_state_upper_bytes())
            .sum::<usize>();
        let sequential_peak = restore_preflights
            .iter()
            .map(|preflight| preflight.sequential_decode_bytes().unwrap())
            .max()
            .unwrap_or(0);
        let raw_payload = restores
            .iter()
            .map(|restore| restore.state.len())
            .chain(whole_restores.iter().map(|restore| restore.state.len()))
            .sum::<usize>();
        let alignment_copy = restores
            .iter()
            .map(|restore| aggregate_checkpoint_alignment_copy_bytes(restore.state))
            .chain(
                whole_restores
                    .iter()
                    .map(|restore| aggregate_checkpoint_alignment_copy_bytes(restore.state)),
            )
            .max()
            .unwrap_or(0);
        let payload_phase = raw_payload
            + alignment_copy
            + aggregate_transition_roster_scratch_bytes(
                predecessor.participants.len(),
                restores.len(),
                restores.len(),
            )
            .unwrap();
        let topology_upper =
            target_owners.len() * (std::mem::size_of::<NodeId>() + std::mem::size_of::<u64>());
        let decode_peak = target_operator.checked_live_state_bytes().unwrap()
            + payload_phase
            + aggregate
                .vnode_transition_restore_roster_bytes(restores.len(), revoked.len())
                .unwrap()
            + topology_upper
            + replacement_upper
            + sequential_peak;
        target_operator.set_managed_state_budget(decode_peak - 1);
        assert!(matches!(
            target_operator.prepare_vnode_transition(transition()),
            Err(DbError::ManagedStateBudgetExceeded {
                accounted_bytes,
                limit_bytes,
                ..
            }) if accounted_bytes == decode_peak && limit_bytes == decode_peak - 1
        ));
        assert!(target_operator.prepared_vnode_transition.is_none());

        target_operator.set_managed_state_budget(usize::MAX);
        target_operator
            .prepare_vnode_transition(transition())
            .unwrap();
        target_operator.publish_vnode_transition();
        assert_eq!(target_operator.local_frontier, frontier);
        assert_eq!(target_operator.effective_frontier, frontier);
        assert!(target_operator.cluster_peers.is_empty());
        assert!(target_operator.peer_channels.is_empty());
        target_operator.finish_vnode_transition();
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aggregate_transition_restores_unaligned_vnode_archive_with_bounded_copy() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

        let predecessor_owners = [2, 1, 1, 1, 1, 1, 1, 1];
        let target_owners = [1, 1, 1, 1, 1, 1, 1, 1];
        let scope = cluster_scope(predecessor_owners).await;
        let (context, _) = context_and_batch();
        let key_groups = KeyGroupCount::try_from(8_u16).unwrap();

        let mut donor = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context.clone(),
            None,
            false,
            key_groups,
        );
        donor.initialize_managed_state().await.unwrap();
        let required = (0..8).collect::<Vec<_>>();
        let donor_vnode = donor
            .checkpoint_vnodes(&required, 8, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .find(|frame| frame.vnode == 0)
            .unwrap();
        let capture = donor_vnode.state.unwrap();
        let mut staged_bytes = capture.retained_bytes();
        let donor_vnode = capture.materialize(&mut staged_bytes, u64::MAX).unwrap();
        let donor_vnode = unaligned_aggregate_archive_transport(&donor_vnode);

        let mut target_operator = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
            key_groups,
        );
        target_operator.initialize_managed_state().await.unwrap();
        target_operator.attach_cluster_shuffle(scope.clone());

        let participant = |node_id| CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
        };
        let predecessor_version = scope.registry.assignment_version();
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            predecessor_version,
            &predecessor_owners,
            vec![participant(1), participant(2)],
        )
        .unwrap();
        let target = CheckpointAssignmentFence::from_owner_map(
            predecessor_version + 1,
            &target_owners,
            vec![participant(1)],
        )
        .unwrap();
        scope.registry.set_assignment_and_version(
            Arc::from(target_owners.map(NodeId)),
            target.assignment_version,
        );
        scope
            .sender
            .install_assignment_fence(&target, &target_owners)
            .unwrap();
        scope
            .receiver
            .install_assignment_fence(&target, &target_owners)
            .unwrap();

        let restores = [ManagedVnodeRestore {
            participant_id: 2,
            vnode: 0,
            state: &donor_vnode,
        }];
        let donor_whole = portable_aggregate_whole_frame(&predecessor, 2, InputFrontier::default());
        let whole_restores = [crate::operator_graph::ManagedWholeRestore {
            participant_id: 2,
            state: &donor_whole,
        }];
        let revoked = rustc_hash::FxHashSet::default();
        let payload_phase_bytes = donor_vnode
            .len()
            .checked_add(donor_whole.len())
            .and_then(|bytes| {
                bytes.checked_add(
                    aggregate_checkpoint_alignment_copy_bytes(&donor_vnode)
                        .max(aggregate_checkpoint_alignment_copy_bytes(&donor_whole)),
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    aggregate_transition_roster_scratch_bytes(
                        predecessor.participants.len(),
                        restores.len(),
                        restores.len(),
                    )
                    .unwrap(),
                )
            })
            .unwrap();
        let payload_limit = payload_phase_bytes - 1;
        target_operator.set_managed_state_budget(payload_limit);
        let error = target_operator
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target,
                revoked: &revoked,
                restores: &restores,
                whole_restores: &whole_restores,
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap_err();
        match error {
            DbError::ManagedStateBudgetExceeded {
                accounted_bytes,
                limit_bytes,
                ..
            } => {
                assert_eq!(accounted_bytes, payload_phase_bytes);
                assert_eq!(limit_bytes, payload_limit);
            }
            other => panic!("unaligned aggregate transition returned the wrong error: {other}"),
        }
        assert!(target_operator.prepared_vnode_transition.is_none());

        target_operator.set_managed_state_budget(usize::MAX);
        target_operator
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target,
                revoked: &revoked,
                restores: &restores,
                whole_restores: &whole_restores,
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        assert!(target_operator.managed_state_accounting().unwrap().prepared > 0);
        target_operator.publish_vnode_transition();
        assert_eq!(
            target_operator
                .cluster_assignment
                .as_ref()
                .unwrap()
                .version(),
            target.assignment_version
        );
        target_operator.finish_vnode_transition();
    }

    #[tokio::test]
    async fn derived_aggregate_requires_incremental_execution() {
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "ratio",
            "SELECT SUM(value) / COUNT(value) AS ratio FROM events",
            context,
            None,
            false,
        );

        let error = operator.initialize_managed_state().await.unwrap_err();
        assert!(matches!(error, DbError::Unsupported(_)));
        assert!(format!("{error}").contains(laminar_core::error_codes::SQL_UNSUPPORTED));
    }

    #[test]
    fn stateful_apply_classification_preserves_stronger_dispositions() {
        let ordinary = stateful_apply_outcome_unknown(
            "totals",
            "state update",
            DbError::Pipeline("injected update failure".into()),
        );
        assert!(matches!(ordinary, DbError::StatefulOperatorPartialApply(_)));
        assert!(ordinary.requires_pipeline_recovery());

        let recovery = stateful_apply_outcome_unknown(
            "totals",
            "state update",
            DbError::Checkpoint("injected recovery".into()),
        );
        assert!(matches!(recovery, DbError::Checkpoint(_)));

        let halt = stateful_apply_outcome_unknown(
            "totals",
            "state update",
            DbError::BackpressureFail("injected halt".into()),
        );
        assert!(matches!(halt, DbError::BackpressureFail(_)));
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn aggregate_shuffle_wrappers_preserve_terminal_disposition() {
        fn assert_terminal(error: DbError, expected: &str) {
            let DbError::ShuffleTerminal(reason) = error else {
                panic!("expected permanent shuffle halt, got {error}");
            };
            assert_eq!(reason, expected);
        }

        let operator = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            laminar_sql::create_session_context(),
            None,
            false,
        );
        assert_terminal(
            operator.remote_replay_error(DbError::ShuffleTerminal("remote replay".into())),
            "remote replay",
        );
        assert_terminal(
            operator.outbound_finalize_error(DbError::ShuffleTerminal("outbound".into())),
            "outbound",
        );
    }

    #[tokio::test]
    async fn later_aggregate_batch_failure_requires_recovery_after_prior_mutation() {
        let (context, seed) = context_and_batch();
        let schema = seed.schema();
        let first = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let later = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["b", "c"])),
                Arc::new(Int64Array::from(vec![2, 3])),
            ],
        )
        .unwrap();
        let mut operator = SqlQueryOperator::new(
            "totals",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );
        operator.lazy_init().await.unwrap();
        let QueryState::Agg(ref mut aggregate) = operator.state else {
            panic!("expected incremental aggregate state");
        };
        aggregate.set_max_groups_for_test(2);

        let error = operator
            .process(&[vec![first, later]], &[i64::MIN])
            .await
            .expect_err("the later batch must exceed the aggregate group limit");

        assert!(matches!(
            &error,
            DbError::StatefulOperatorPartialApply(message)
                if message.contains("state update") && message.contains("outcome is unknown")
        ));
        assert!(error.requires_pipeline_recovery());
    }

    #[test]
    fn corrupt_aggregate_checkpoint_is_a_recovery_fault() {
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );
        let error = operator
            .restore(OperatorCheckpoint {
                data: b"not-rkyv".to_vec(),
            })
            .unwrap_err();
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.requires_pipeline_recovery());
    }

    #[tokio::test]
    async fn vnode_capture_is_incremental_and_restores_unaligned_without_whole_state() {
        let (context, batch) = context_and_batch();
        let key_groups = KeyGroupCount::try_from(8_u16).unwrap();
        let mut donor = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context.clone(),
            None,
            false,
            key_groups,
        );
        donor.initialize_managed_state().await.unwrap();
        donor.process(&[vec![batch]], &[100]).await.unwrap();
        let owned = (0..8).collect::<Vec<_>>();
        let baseline = donor
            .checkpoint_vnodes(&owned, 8, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(baseline.len(), owned.len());
        assert!(baseline.iter().all(|frame| frame.state.is_some()));
        assert!(donor
            .checkpoint_vnodes(&owned, 8, u64::MAX)
            .unwrap()
            .unwrap()
            .is_empty());

        let frames = baseline
            .into_iter()
            .map(|frame| {
                let capture = frame.state.unwrap();
                let mut staged_bytes = capture.retained_bytes();
                let state = capture.materialize(&mut staged_bytes, u64::MAX).unwrap();
                (frame.vnode, unaligned_aggregate_archive_transport(&state))
            })
            .collect::<Vec<_>>();
        assert!(frames.iter().all(|(_, state)| {
            state
                .as_ptr()
                .align_offset(AGG_CHECKPOINT_ARCHIVE_ALIGNMENT)
                != 0
        }));

        let mut limited = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context.clone(),
            None,
            false,
            key_groups,
        );
        limited.initialize_managed_state().await.unwrap();
        let (limited_vnode, limited_state) = &frames[0];
        let alignment_accounted = limited_state.len().checked_mul(2).unwrap();
        let limit = alignment_accounted - 1;
        limited.set_managed_state_budget(limit);
        let error = limited
            .restore_vnode(*limited_vnode, 8, limited_state)
            .unwrap_err();
        match error {
            DbError::ManagedStateBudgetExceeded {
                accounted_bytes,
                limit_bytes,
                ..
            } => {
                assert_eq!(accounted_bytes, alignment_accounted);
                assert_eq!(limit_bytes, limit);
            }
            other => panic!("unaligned aggregate restore returned the wrong error: {other}"),
        }
        let QueryState::Agg(limited_state) = &limited.state else {
            panic!("expected limited aggregate state");
        };
        assert_eq!(limited_state.logical_group_count_for_test(), 0);

        let (decode_vnode, decode_state) = frames
            .iter()
            .find(|(_, state)| {
                let QueryState::Agg(aggregate) = &limited.state else {
                    return false;
                };
                let profile = aggregate.vnode_archive_restore_profile();
                with_aligned_aggregate_checkpoint_bytes(state, |state| {
                    profile
                        .preflight(state, format_args!("decode-bound test"))
                        .map(|archive| archive.group_count() != 0)
                })
                .unwrap()
            })
            .expect("captured aggregate has a nonempty vnode");
        let mut decode_limited = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context.clone(),
            None,
            false,
            key_groups,
        );
        decode_limited.initialize_managed_state().await.unwrap();
        let QueryState::Agg(aggregate) = &decode_limited.state else {
            panic!("expected decode-limited aggregate state");
        };
        let profile = aggregate.vnode_archive_restore_profile();
        let restore_preflight = with_aligned_aggregate_checkpoint_bytes(decode_state, |state| {
            profile
                .preflight(state, format_args!("decode-bound test"))
                .map(|archive| archive.restore_preflight())
        })
        .unwrap();
        let decode_peak = aggregate
            .accounted_state_bytes()
            .checked_add(decode_state.len())
            .and_then(|bytes| {
                bytes.checked_add(aggregate_checkpoint_alignment_copy_bytes(decode_state))
            })
            .and_then(|bytes| {
                aggregate
                    .vnode_transition_restore_roster_bytes(1, 0)
                    .ok()
                    .and_then(|roster| bytes.checked_add(roster))
            })
            .and_then(|bytes| {
                restore_preflight
                    .sequential_decode_bytes()
                    .and_then(|decode| bytes.checked_add(decode))
            })
            .and_then(|bytes| bytes.checked_add(restore_preflight.final_state_upper_bytes()))
            .unwrap();
        decode_limited.set_managed_state_budget(decode_peak - 1);
        let error = decode_limited
            .restore_vnode(*decode_vnode, 8, decode_state)
            .unwrap_err();
        assert!(matches!(
            error,
            DbError::ManagedStateBudgetExceeded {
                accounted_bytes,
                limit_bytes,
                ..
            } if accounted_bytes == decode_peak && limit_bytes == decode_peak - 1
        ));
        let QueryState::Agg(aggregate) = &decode_limited.state else {
            panic!("expected decode-limited aggregate state");
        };
        assert_eq!(aggregate.logical_group_count_for_test(), 0);

        let mut restored = SqlQueryOperator::new_with_key_groups(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
            key_groups,
        );
        restored.initialize_managed_state().await.unwrap();
        for (vnode, state) in &frames {
            restored.restore_vnode(*vnode, 8, state).unwrap();
        }
        let QueryState::Agg(aggregate) = &restored.state else {
            panic!("expected restored aggregate state");
        };
        assert_eq!(aggregate.logical_group_count_for_test(), 2);
        let expected = donor.process(&[Vec::new()], &[200]).await.unwrap();
        let actual = restored.process(&[Vec::new()], &[200]).await.unwrap();
        assert_eq!(actual, expected);

        donor.force_full_vnode_capture();
        let forced = donor
            .checkpoint_vnodes(&owned, 8, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(forced.len(), owned.len());
        assert!(forced.iter().all(|frame| frame.state.is_some()));
    }

    #[cfg(not(feature = "cluster"))]
    #[test]
    fn cluster_shuffle_checkpoint_is_rejected_without_support() {
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new(
            "sum",
            "SELECT key, SUM(value) AS total FROM events GROUP BY key",
            context,
            None,
            false,
        );
        let error = operator
            .restore(OperatorCheckpoint { data: Vec::new() })
            .unwrap_err();
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.to_string().contains("cluster support"));
    }
}
