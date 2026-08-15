//! EOWC (Emit On Window Close) operator backed by `CoreWindowState`.

#[cfg(feature = "cluster")]
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

#[cfg(feature = "cluster")]
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionContext;
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::CheckpointAssignmentFence;
#[cfg(feature = "cluster")]
use laminar_core::shuffle::ShuffleMessage;
use laminar_core::state::KeyGroupCount;
#[cfg(feature = "cluster")]
use laminar_core::state::{NodeId, VnodeAssignmentSnapshot};

use crate::core_window_state::{CoreWindowState, CoreWindowVnodeCheckpoint};
#[cfg(feature = "cluster")]
use crate::core_window_state::{
    PreparedCoreWindowVnodeTransition, RetiredCoreWindowVnodeTransition,
};
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::operator::capability::{ManagedStateContract, OperatorCapability};
#[cfg(feature = "cluster")]
use crate::operator::sql_query::ClusterShuffleConfig;
#[cfg(feature = "cluster")]
use crate::operator_graph::merge_input_frontier_iter;
use crate::operator_graph::{
    try_evaluate_compiled, EncodedStateFrame, GraphOperator, InputFrontier,
    ManagedStateAccountingSnapshot, OperatorCheckpoint, StateFrameCapture,
};
#[cfg(feature = "cluster")]
use crate::operator_graph::{ManagedVnodeTransition, ManagedVnodeTransitionMode};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::WindowOperatorConfig;

#[cfg(feature = "cluster")]
struct EowcTransitionTopology {
    assignment: VnodeAssignmentSnapshot,
    assignment_digest: [u8; 32],
    peers: Arc<[u64]>,
    channels: BTreeMap<u64, EowcPeerChannel>,
    local_frontier: InputFrontier,
    last_broadcast: InputFrontier,
    effective_frontier: InputFrontier,
    remote_peer_cursor: Option<u64>,
    queued_payload_bytes: usize,
    queued_event_capacity_bytes: usize,
    queued_remote_events: usize,
}

#[cfg(feature = "cluster")]
struct PreparedEowcTransition {
    core: PreparedCoreWindowVnodeTransition,
    topology: EowcTransitionTopology,
}

#[cfg(feature = "cluster")]
struct RetiredEowcTransition {
    core: RetiredCoreWindowVnodeTransition,
    topology: EowcTransitionTopology,
}

#[cfg(feature = "cluster")]
enum CoreWindowTransitionCleanup {
    Aborted(PreparedEowcTransition),
    Published(RetiredEowcTransition),
}

const OPERATOR_CHECKPOINT_VERSION: u8 = 2;
const OPERATOR_CHECKPOINT_BASE_SCRATCH: usize = 512;
const CHECKPOINT_ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;
#[cfg(feature = "cluster")]
const OPERATOR_CAPTURE_ALLOCATION_CHARGE: usize = 32;

fn checkpoint_alignment_copy_bytes(bytes: &[u8]) -> usize {
    if bytes.as_ptr().align_offset(CHECKPOINT_ARCHIVE_ALIGNMENT) == 0 {
        0
    } else {
        bytes.len()
    }
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
struct EowcCheckpointFrontier {
    watermark: Option<i64>,
    idle: bool,
}

impl From<InputFrontier> for EowcCheckpointFrontier {
    fn from(frontier: InputFrontier) -> Self {
        Self {
            watermark: frontier.watermark,
            idle: frontier.idle,
        }
    }
}

impl From<EowcCheckpointFrontier> for InputFrontier {
    fn from(frontier: EowcCheckpointFrontier) -> Self {
        Self {
            watermark: frontier.watermark,
            idle: frontier.idle,
        }
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct EowcCheckpointChannel {
    peer: u64,
    applied: EowcCheckpointFrontier,
    events: Vec<EowcCheckpointEvent>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
enum EowcCheckpointEvent {
    Data {
        recovery_gen: u64,
        routed_vnodes: Vec<u32>,
        row_count: u64,
    },
    Frontier {
        recovery_gen: u64,
        frontier: EowcCheckpointFrontier,
    },
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct EowcClusterCheckpoint {
    assignment_version: u64,
    owner_map_digest: [u8; 32],
    self_id: u64,
    local_frontier: EowcCheckpointFrontier,
    effective_frontier: EowcCheckpointFrontier,
    remote_peer_cursor: Option<u64>,
    channels: Vec<EowcCheckpointChannel>,
    data_ipc: Vec<u8>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct EowcOperatorCheckpoint {
    version: u8,
    high_watermark_ms: i64,
    cluster: Option<EowcClusterCheckpoint>,
}

#[cfg(feature = "cluster")]
#[derive(Clone)]
struct EowcQueuedBatch {
    retained: Arc<crate::operator::RetainedBatch>,
    row_vnodes: Arc<[u32]>,
    charged_bytes: usize,
}

#[cfg(feature = "cluster")]
#[derive(Clone)]
enum EowcRemoteEventPayload {
    Data(EowcQueuedBatch),
    Frontier(InputFrontier),
}

#[cfg(feature = "cluster")]
#[derive(Clone)]
struct EowcRemoteEvent {
    assignment_version: u64,
    recovery_gen: u64,
    payload: EowcRemoteEventPayload,
}

#[cfg(feature = "cluster")]
impl EowcRemoteEvent {
    fn payload_bytes(&self) -> usize {
        match &self.payload {
            EowcRemoteEventPayload::Data(batch) => batch.charged_bytes,
            EowcRemoteEventPayload::Frontier(_) => 0,
        }
    }
}

#[cfg(feature = "cluster")]
#[derive(Default)]
struct EowcPeerChannel {
    applied: InputFrontier,
    accepted: InputFrontier,
    events: VecDeque<EowcRemoteEvent>,
}

#[cfg(feature = "cluster")]
struct EowcClusterInputPlan {
    local_batches: Vec<(RecordBatch, Option<u32>)>,
    outbound: Vec<(u64, ShuffleMessage)>,
    local_frontier: InputFrontier,
    effective_frontier: InputFrontier,
}

#[cfg(feature = "cluster")]
type EowcSendTask =
    tokio::task::JoinHandle<(Result<(), DbError>, Option<Vec<(u64, ShuffleMessage)>>)>;

#[cfg(feature = "cluster")]
struct PendingEowcClusterInput {
    local_batches: Vec<(RecordBatch, Option<u32>)>,
    outbound: Option<Vec<(u64, ShuffleMessage)>>,
    local_frontier: InputFrontier,
    send: Option<EowcSendTask>,
    accounted_bytes: usize,
}

#[cfg(feature = "cluster")]
enum PendingEowcCompletion {
    Waiting,
    RetryLater,
    Applied(Vec<RecordBatch>),
}

#[cfg(feature = "cluster")]
impl Drop for PendingEowcClusterInput {
    fn drop(&mut self) {
        if let Some(send) = &self.send {
            send.abort();
        }
    }
}

#[cfg(feature = "cluster")]
enum CapturedEowcEvent {
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
struct CapturedEowcChannel {
    peer: u64,
    applied: InputFrontier,
    events: Vec<CapturedEowcEvent>,
}

#[cfg(feature = "cluster")]
struct CapturedEowcCluster {
    assignment_version: u64,
    owner_map_digest: [u8; 32],
    self_id: u64,
    local_frontier: InputFrontier,
    effective_frontier: InputFrontier,
    remote_peer_cursor: Option<u64>,
    channels: Vec<CapturedEowcChannel>,
    retained_bytes: usize,
}

struct EowcOperatorCheckpointCapture {
    high_watermark_ms: i64,
    #[cfg(feature = "cluster")]
    cluster: Option<CapturedEowcCluster>,
    retained_bytes: u64,
}

#[cfg(feature = "cluster")]
struct DecodedEowcCluster {
    local_frontier: InputFrontier,
    effective_frontier: InputFrontier,
    remote_peer_cursor: Option<u64>,
    peer_channels: BTreeMap<u64, EowcPeerChannel>,
    queued_payload_bytes: usize,
    queued_event_capacity_bytes: usize,
    queued_remote_events: usize,
}

#[cfg(feature = "cluster")]
const REMOTE_EVENT_CHARGE: usize = std::mem::size_of::<EowcRemoteEvent>();
#[cfg(feature = "cluster")]
const RETAINED_BATCH_ARC_CHARGE: usize =
    std::mem::size_of::<Arc<crate::operator::RetainedBatch>>() + 2 * std::mem::size_of::<usize>();
#[cfg(feature = "cluster")]
const ROW_VNODE_ARC_CHARGE: usize = 2 * std::mem::size_of::<usize>();
#[cfg(feature = "cluster")]
const PEER_CHANNEL_ENTRY_CHARGE: usize = 64;

#[cfg(feature = "cluster")]
impl EowcTransitionTopology {
    fn accounted_state_bytes(&self) -> usize {
        let assignment = self.assignment.owners().len().saturating_mul(
            std::mem::size_of::<NodeId>().saturating_add(std::mem::size_of::<u64>()),
        );
        let peers = self.peers.len().saturating_mul(std::mem::size_of::<u64>());
        let channels = self
            .channels
            .len()
            .saturating_mul(
                std::mem::size_of::<(u64, EowcPeerChannel)>()
                    .saturating_add(PEER_CHANNEL_ENTRY_CHARGE),
            )
            .saturating_add(self.queued_event_capacity_bytes)
            .saturating_add(self.queued_payload_bytes);
        std::mem::size_of::<Self>()
            .saturating_add(assignment)
            .saturating_add(peers)
            .saturating_add(channels)
    }
}

#[cfg(feature = "cluster")]
impl PreparedEowcTransition {
    fn accounted_state_bytes(&self) -> usize {
        self.core
            .accounted_state_bytes()
            .saturating_add(self.topology.accounted_state_bytes())
    }
}

#[cfg(feature = "cluster")]
impl RetiredEowcTransition {
    fn accounted_state_bytes(&self) -> usize {
        self.core
            .accounted_state_bytes()
            .saturating_add(self.topology.accounted_state_bytes())
    }
}

impl EowcOperatorCheckpointCapture {
    fn encode(self, max_encoded_bytes: usize) -> Result<Vec<u8>, DbError> {
        let remaining = max_encoded_bytes
            .checked_sub(OPERATOR_CHECKPOINT_BASE_SCRATCH)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "EOWC operator checkpoint scratch requires {OPERATOR_CHECKPOINT_BASE_SCRATCH} bytes; encoding headroom is {max_encoded_bytes} bytes"
                ))
            })?;
        #[cfg(feature = "cluster")]
        let (cluster, remaining) = if let Some(cluster) = self.cluster {
            let (cluster, next) = cluster.encode(remaining)?;
            (Some(cluster), next)
        } else {
            (None, remaining)
        };
        #[cfg(not(feature = "cluster"))]
        let cluster = None;
        let checkpoint = EowcOperatorCheckpoint {
            version: OPERATOR_CHECKPOINT_VERSION,
            high_watermark_ms: self.high_watermark_ms,
            cluster,
        };
        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(remaining),
        );
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&checkpoint, writer)
            .map(|bytes| bytes.into_inner().into_vec())
            .map_err(|error| DbError::Checkpoint(format!("EOWC operator checkpoint: {error}")))
    }
}

#[cfg(feature = "cluster")]
impl CapturedEowcCluster {
    fn encoding_scratch_bytes(&self) -> Result<usize, DbError> {
        let allocation = |bytes: usize| {
            bytes.checked_add(usize::from(bytes != 0) * OPERATOR_CAPTURE_ALLOCATION_CHARGE)
        };
        let mut bytes = allocation(
            self.channels
                .len()
                .checked_mul(std::mem::size_of::<EowcCheckpointChannel>())
                .ok_or_else(|| {
                    DbError::Checkpoint("EOWC channel checkpoint scratch overflow".into())
                })?,
        )
        .ok_or_else(|| DbError::Checkpoint("EOWC channel checkpoint scratch overflow".into()))?;
        for channel in &self.channels {
            bytes = bytes
                .checked_add(
                    allocation(
                        channel
                            .events
                            .len()
                            .checked_mul(std::mem::size_of::<EowcCheckpointEvent>())
                            .ok_or_else(|| {
                                DbError::Checkpoint(
                                    "EOWC channel checkpoint scratch overflow".into(),
                                )
                            })?,
                    )
                    .ok_or_else(|| {
                        DbError::Checkpoint("EOWC channel checkpoint scratch overflow".into())
                    })?,
                )
                .ok_or_else(|| {
                    DbError::Checkpoint("EOWC channel checkpoint scratch overflow".into())
                })?;
            for event in &channel.events {
                if let CapturedEowcEvent::Data { retained, .. } = event {
                    bytes = bytes
                        .checked_add(
                            allocation(
                                retained
                                    .routed_vnodes()
                                    .len()
                                    .checked_mul(std::mem::size_of::<u32>())
                                    .ok_or_else(|| {
                                        DbError::Checkpoint(
                                            "EOWC channel checkpoint scratch overflow".into(),
                                        )
                                    })?,
                            )
                            .ok_or_else(|| {
                                DbError::Checkpoint(
                                    "EOWC channel checkpoint scratch overflow".into(),
                                )
                            })?,
                        )
                        .ok_or_else(|| {
                            DbError::Checkpoint("EOWC channel checkpoint scratch overflow".into())
                        })?;
                    bytes = bytes
                        .checked_add(retained.heap_bytes().ok_or_else(|| {
                            DbError::Checkpoint("EOWC channel checkpoint scratch overflow".into())
                        })?)
                        .ok_or_else(|| {
                            DbError::Checkpoint("EOWC channel checkpoint scratch overflow".into())
                        })?;
                }
            }
        }
        Ok(bytes)
    }

    fn encode(self, max_encoded_bytes: usize) -> Result<(EowcClusterCheckpoint, usize), DbError> {
        let scratch_bytes = self.encoding_scratch_bytes()?;
        let mut remaining = max_encoded_bytes.checked_sub(scratch_bytes).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "EOWC channel checkpoint scratch requires {scratch_bytes} bytes; encoding headroom is {max_encoded_bytes} bytes"
            ))
        })?;
        let first_batch = self.channels.iter().find_map(|channel| {
            channel.events.iter().find_map(|event| match event {
                CapturedEowcEvent::Data { retained, .. } => Some(retained.batch()),
                CapturedEowcEvent::Frontier { .. } => None,
            })
        });
        let data_ipc = if let Some(first_batch) = first_batch {
            let batches = self.channels.iter().flat_map(|channel| {
                channel.events.iter().filter_map(|event| match event {
                    CapturedEowcEvent::Data { retained, .. } => Some(retained.batch()),
                    CapturedEowcEvent::Frontier { .. } => None,
                })
            });
            let ipc = laminar_core::serialization::serialize_batches_stream_bounded(
                first_batch.schema().as_ref(),
                batches,
                remaining,
            )
            .map_err(|error| DbError::Checkpoint(format!("EOWC channel IPC: {error}")))?;
            remaining = remaining.checked_sub(ipc.capacity()).ok_or_else(|| {
                DbError::Checkpoint("EOWC channel IPC exceeded its encoding budget".into())
            })?;
            ipc
        } else {
            Vec::new()
        };

        let mut channels = Vec::with_capacity(self.channels.len());
        for channel in self.channels {
            let mut events = Vec::with_capacity(channel.events.len());
            for event in channel.events {
                events.push(match event {
                    CapturedEowcEvent::Data {
                        recovery_gen,
                        retained,
                    } => EowcCheckpointEvent::Data {
                        recovery_gen,
                        routed_vnodes: retained.routed_vnodes().to_vec(),
                        row_count: u64::try_from(retained.batch().num_rows()).map_err(|_| {
                            DbError::Checkpoint("EOWC channel row count exceeds u64".into())
                        })?,
                    },
                    CapturedEowcEvent::Frontier {
                        recovery_gen,
                        frontier,
                    } => EowcCheckpointEvent::Frontier {
                        recovery_gen,
                        frontier: frontier.into(),
                    },
                });
            }
            channels.push(EowcCheckpointChannel {
                peer: channel.peer,
                applied: channel.applied.into(),
                events,
            });
        }
        Ok((
            EowcClusterCheckpoint {
                assignment_version: self.assignment_version,
                owner_map_digest: self.owner_map_digest,
                self_id: self.self_id,
                local_frontier: self.local_frontier.into(),
                effective_frontier: self.effective_frontier.into(),
                remote_peer_cursor: self.remote_peer_cursor,
                channels,
                data_ipc,
            },
            remaining,
        ))
    }
}

/// EOWC query operator: suppresses intermediate results and emits only
/// when windows close.
pub(crate) struct EowcQueryOperator {
    op_name: Arc<str>,
    sql: Arc<str>,
    emit_clause: Option<EmitClause>,
    window_config: Option<WindowOperatorConfig>,
    ctx: SessionContext,
    task_ctx: Arc<TaskContext>,
    key_group_count: KeyGroupCount,
    capability: OperatorCapability,
    state: Option<Box<CoreWindowState>>,
    whole_restore_applied: bool,
    max_managed_state_bytes: usize,
    prom: Option<Arc<EngineMetrics>>,
    #[cfg(feature = "cluster")]
    cluster_scope: Option<ClusterShuffleConfig>,
    #[cfg(feature = "cluster")]
    cluster_assignment: Option<VnodeAssignmentSnapshot>,
    #[cfg(feature = "cluster")]
    cluster_assignment_digest: Option<[u8; 32]>,
    #[cfg(feature = "cluster")]
    cluster_peers: Arc<[u64]>,
    #[cfg(feature = "cluster")]
    peer_channels: BTreeMap<u64, EowcPeerChannel>,
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
    pending_cluster_input: Option<PendingEowcClusterInput>,
    #[cfg(feature = "cluster")]
    prepared_vnode_transition: Option<PreparedEowcTransition>,
    #[cfg(feature = "cluster")]
    vnode_transition_cleanup: Option<CoreWindowTransitionCleanup>,
}

impl EowcQueryOperator {
    pub fn new(
        name: &str,
        sql: &str,
        emit_clause: Option<EmitClause>,
        window_config: Option<WindowOperatorConfig>,
        ctx: SessionContext,
        key_group_count: KeyGroupCount,
        prom: Option<Arc<EngineMetrics>>,
    ) -> Self {
        let task_ctx = ctx.task_ctx();
        Self {
            op_name: Arc::from(name),
            sql: Arc::from(sql),
            emit_clause,
            window_config,
            ctx,
            task_ctx,
            key_group_count,
            capability: OperatorCapability::managed_core_window(),
            state: None,
            whole_restore_applied: false,
            max_managed_state_bytes: crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            prom,
            #[cfg(feature = "cluster")]
            cluster_scope: None,
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
            prepared_vnode_transition: None,
            #[cfg(feature = "cluster")]
            vnode_transition_cleanup: None,
        }
    }

    async fn initialize(&mut self) -> Result<(), DbError> {
        let cfg = self.window_config.as_ref().ok_or_else(|| {
            DbError::Unsupported(format!(
                "[LDB-1001] EOWC query '{}' requires a supported TUMBLE, HOP, or SESSION aggregate",
                self.op_name
            ))
        })?;
        #[cfg(feature = "cluster")]
        if self.cluster_scope.is_some()
            && crate::sql_analysis::managed_core_window_source(&self.sql, cfg).is_none()
        {
            return Err(DbError::Unsupported(format!(
                "[{}] EOWC query '{}' is outside the certified direct-source CoreWindow shape",
                laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED,
                self.op_name
            )));
        }
        let Some(mut window) = CoreWindowState::try_from_sql(
            &self.ctx,
            &self.sql,
            cfg,
            self.emit_clause.as_ref(),
            self.key_group_count,
        )
        .await?
        else {
            return Err(DbError::Unsupported(format!(
                "[LDB-1001] EOWC query '{}' is not a supported TUMBLE, HOP, or SESSION aggregate",
                self.op_name
            )));
        };

        #[cfg(feature = "cluster")]
        if self.cluster_scope.is_some() && !window.planned_functions_are_immutable() {
            return Err(DbError::Unsupported(format!(
                "[{}] EOWC query '{}' contains a planned function that is not replay-immutable",
                laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED,
                self.op_name
            )));
        }
        #[cfg(feature = "cluster")]
        if self.cluster_scope.is_some() && window.compiled_projection().is_none() {
            return Err(DbError::Unsupported(format!(
                "[{}] EOWC query '{}' has no compiled pre-aggregation path",
                laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED,
                self.op_name
            )));
        }

        window.attach_metrics(self.prom.clone());
        tracing::info!(
            query = %self.op_name,
            window_type = ?cfg.window_type,
            "EOWC operator: initialized core window state"
        );
        self.state = Some(Box::new(window));
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn attach_cluster_scope(&mut self, scope: ClusterShuffleConfig) {
        debug_assert!(self.cluster_scope.is_none());
        debug_assert_eq!(
            scope.registry.vnode_count(),
            u32::from(self.key_group_count)
        );
        let assignment = scope.registry.versioned_snapshot();
        let peers = Self::remote_owner_peers(&assignment, scope.self_id);
        self.cluster_assignment_digest = Some(self.owner_map_digest(&assignment));
        self.peer_channels = peers
            .iter()
            .copied()
            .map(|peer| (peer, EowcPeerChannel::default()))
            .collect();
        self.cluster_peers = peers.into();
        self.cluster_assignment = Some(assignment);
        self.cluster_scope = Some(scope);
    }

    fn core_window_apply_error(op_name: &str, phase: &str, error: DbError) -> DbError {
        if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
            return error;
        }
        DbError::StatefulOperatorPartialApply(format!(
            "managed CoreWindow '{op_name}' {phase} failed after window state mutation began; recovery from the committed checkpoint is required: {error}"
        ))
    }

    async fn pre_aggregate(
        cw: &mut CoreWindowState,
        inputs: &[RecordBatch],
        watermark: i64,
        op_name: &str,
        ctx: &SessionContext,
        task_ctx: &Arc<TaskContext>,
        require_compiled: bool,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let now_filtered = cw.apply_dynamic_now_filter(ctx, inputs, watermark)?;
        let inputs: &[RecordBatch] = now_filtered.as_deref().unwrap_or(inputs);

        let batches = if let Some(proj) = cw.compiled_projection() {
            match try_evaluate_compiled(proj, inputs) {
                Ok(result) => result,
                Err(error) if require_compiled => {
                    return Err(DbError::PipelineTerminal(format!(
                        "managed CoreWindow '{op_name}' compiled pre-aggregation failed: {error}"
                    )));
                }
                Err(e) => {
                    tracing::debug!(
                        query = %op_name,
                        error = %e,
                        "EOWC compiled pre-agg failed, falling back to cached plan"
                    );
                    if let Some(physical) = cw.cached_pre_agg_physical() {
                        super::execute_cached_physical(task_ctx.clone(), op_name, physical).await?
                    } else {
                        return Err(DbError::Pipeline(format!(
                            "[LDB-8051] EOWC query '{op_name}': compiled pre-agg failed and no cached plan: {e}"
                        )));
                    }
                }
            }
        } else if require_compiled {
            return Err(DbError::PipelineTerminal(format!(
                "managed CoreWindow '{op_name}' has no compiled pre-aggregation"
            )));
        } else if let Some(physical) = cw.cached_pre_agg_physical() {
            super::execute_cached_physical(task_ctx.clone(), op_name, physical).await?
        } else {
            return Err(DbError::Pipeline(format!(
                "[LDB-8050] EOWC query '{op_name}': no compiled projection or cached plan"
            )));
        };
        Ok(batches)
    }

    fn apply_routed_and_close(
        cw: &mut CoreWindowState,
        batches: &[(RecordBatch, Option<u32>)],
        watermark: i64,
        op_name: &str,
    ) -> Result<Vec<RecordBatch>, DbError> {
        for (batch, vnode) in batches {
            if let Err(error) = cw.update_batch_for_vnode(batch, *vnode) {
                return Err(Self::core_window_apply_error(
                    op_name,
                    "state update",
                    error,
                ));
            }
        }

        cw.close_windows(watermark)
            .map_err(|error| Self::core_window_apply_error(op_name, "window close", error))
    }

    const fn frontier_watermark(frontier: InputFrontier) -> i64 {
        match frontier.watermark {
            Some(watermark) => watermark,
            None => i64::MIN,
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
        {
            return Err(DbError::Pipeline(format!(
                "EOWC '{}' {context} frontier became uninitialized",
                self.op_name
            )));
        }
        if let (Some(previous), Some(next)) = (previous.watermark, next.watermark) {
            if next < previous {
                return Err(DbError::Pipeline(format!(
                    "EOWC '{}' {context} frontier regressed from {previous} to {next}",
                    self.op_name
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
        CheckpointAssignmentFence::owner_map_digest(u32::from(self.key_group_count), &owners)
    }

    #[cfg(feature = "cluster")]
    fn active_cluster_scope(
        &self,
    ) -> Result<(ClusterShuffleConfig, VnodeAssignmentSnapshot, Arc<[u64]>), DbError> {
        let config = self.cluster_scope.clone().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' has no cluster shuffle scope",
                self.op_name
            ))
        })?;
        let pinned = self.cluster_assignment.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' has no pinned cluster assignment",
                self.op_name
            ))
        })?;
        self.cluster_assignment_digest.ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' has no pinned assignment digest",
                self.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let sender_digest = config.sender.active_assignment_digest();
        let receiver_digest = config.receiver.active_assignment_digest();
        if u32::try_from(assignment.owners().len()).ok() != Some(u32::from(self.key_group_count))
            || assignment.version() != pinned.version()
            || !std::ptr::eq(assignment.owners(), pinned.owners())
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
                "managed CoreWindow '{}' cluster ownership is outside its attached assignment",
                self.op_name
            )));
        }
        Ok((config, assignment, Arc::clone(&self.cluster_peers)))
    }

    #[cfg(feature = "cluster")]
    fn accounting_error(&self) -> DbError {
        DbError::Pipeline(format!(
            "managed CoreWindow '{}' state accounting overflow",
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
            .checked_mul(std::mem::size_of::<(u64, EowcPeerChannel)>() + PEER_CHANNEL_ENTRY_CHARGE)
            .and_then(|bytes| bytes.checked_add(self.queued_event_capacity_bytes))
            .ok_or_else(|| self.accounting_error())?;
        peers
            .checked_add(channels)
            .ok_or_else(|| self.accounting_error())
    }

    #[cfg(feature = "cluster")]
    fn checked_live_state_bytes(&self) -> Result<usize, DbError> {
        let window = self
            .state
            .as_ref()
            .map_or(0, |window| window.accounted_state_bytes());
        window
            .checked_add(self.cluster_topology_bytes()?)
            .and_then(|bytes| bytes.checked_add(self.queued_payload_bytes))
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
    fn cluster_input_plan_bytes(&self, plan: &EowcClusterInputPlan) -> Result<usize, DbError> {
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
    fn ensure_cluster_restore_budget(
        &self,
        base: usize,
        payload: usize,
        event_capacity: usize,
    ) -> Result<(), DbError> {
        let accounted = base
            .checked_add(payload)
            .and_then(|bytes| bytes.checked_add(event_capacity))
            .ok_or_else(|| self.accounting_error())?;
        if accounted > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "managed CoreWindow '{}' ordered shuffle restore",
                    self.op_name
                ),
                accounted_bytes: accounted,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn reserve_remote_event_slot(
        &mut self,
        peer: u64,
        payload_bytes: usize,
    ) -> Result<(), DbError> {
        let current_accounted = self.checked_live_state_bytes()?;
        let previous_capacity = self.peer_channels[&peer].events.capacity();
        self.peer_channels
            .get_mut(&peer)
            .expect("validated CoreWindow peer channel")
            .events
            .try_reserve_exact(1)
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "managed CoreWindow '{}' could not reserve ordered shuffle event: {error}",
                    self.op_name
                ))
            })?;
        let reserved_capacity = self.peer_channels[&peer].events.capacity();
        let added_capacity_bytes = reserved_capacity
            .checked_sub(previous_capacity)
            .and_then(|slots| slots.checked_mul(REMOTE_EVENT_CHARGE))
            .ok_or_else(|| self.accounting_error())?;
        let next_accounted = current_accounted
            .checked_add(added_capacity_bytes)
            .and_then(|bytes| bytes.checked_add(payload_bytes))
            .ok_or_else(|| self.accounting_error())?;
        if next_accounted > self.max_managed_state_bytes {
            self.peer_channels
                .get_mut(&peer)
                .expect("reserved CoreWindow peer channel")
                .events
                .shrink_to(previous_capacity);
            let retained_capacity = self.peer_channels[&peer]
                .events
                .capacity()
                .saturating_sub(previous_capacity)
                .checked_mul(REMOTE_EVENT_CHARGE)
                .ok_or_else(|| self.accounting_error())?;
            self.queued_event_capacity_bytes = self
                .queued_event_capacity_bytes
                .checked_add(retained_capacity)
                .ok_or_else(|| self.accounting_error())?;
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "managed CoreWindow '{}' ordered shuffle queue",
                    self.op_name
                ),
                accounted_bytes: next_accounted,
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
    fn expected_pre_aggregate_schema(
        window: &CoreWindowState,
    ) -> Result<arrow::datatypes::SchemaRef, DbError> {
        if let Some(projection) = window.compiled_projection() {
            return Ok(Arc::clone(&projection.output_schema));
        }
        window.cached_pre_agg_physical().map_or_else(
            || {
                Err(DbError::Pipeline(
                    "managed CoreWindow has no pre-aggregate schema".into(),
                ))
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
    ) -> Result<EowcQueuedBatch, DbError> {
        if accepted.idle {
            return Err(DbError::ShuffleTerminal(format!(
                "managed CoreWindow '{}' received data while its peer channel was idle",
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
                "managed CoreWindow '{}' rejected non-canonical shuffle data",
                self.op_name
            )));
        }
        let logical_bytes = laminar_core::shuffle::logical_batch_bytes(batch).map_err(|error| {
            DbError::ShuffleTerminal(format!(
                "managed CoreWindow '{}' rejected shuffle batch size: {error}",
                self.op_name
            ))
        })?;
        if logical_bytes > laminar_core::shuffle::ROUTE_MAX_BATCH_BYTES {
            return Err(DbError::ShuffleTerminal(format!(
                "managed CoreWindow '{}' shuffle batch exceeds its route limit",
                self.op_name
            )));
        }
        let window = self.state.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' received shuffle data before initialization",
                self.op_name
            ))
        })?;
        if batch.schema().as_ref() != Self::expected_pre_aggregate_schema(window)?.as_ref() {
            return Err(DbError::ShuffleTerminal(format!(
                "managed CoreWindow '{}' shuffle schema does not match its pre-aggregate schema",
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
                "managed CoreWindow '{}' received data outside its vnode ownership",
                self.op_name
            )));
        }
        let row_vnodes = crate::operator::sql_query::hash_rows_to_vnodes(
            batch,
            window.num_group_cols(),
            u32::from(self.key_group_count),
        )
        .map_err(|error| {
            crate::operator::shuffle_routing_error(
                &format!("managed CoreWindow '{}' received routing", self.op_name),
                &error,
            )
        })?;
        let mut seen = Vec::new();
        seen.try_reserve_exact(retained.routed_vnodes().len())
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "managed CoreWindow '{}' could not reserve route validation: {error}",
                    self.op_name
                ))
            })?;
        seen.resize(retained.routed_vnodes().len(), false);
        for vnode in &row_vnodes {
            let Ok(index) = retained.routed_vnodes().binary_search(vnode) else {
                return Err(DbError::ShuffleTerminal(format!(
                    "managed CoreWindow '{}' shuffle vnode metadata omits a decoded row",
                    self.op_name
                )));
            };
            seen[index] = true;
        }
        if seen.iter().any(|seen| !seen) {
            return Err(DbError::ShuffleTerminal(format!(
                "managed CoreWindow '{}' shuffle vnode metadata names an absent row",
                self.op_name
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
        Ok(EowcQueuedBatch {
            retained: Arc::new(retained),
            row_vnodes: row_vnodes.into(),
            charged_bytes,
        })
    }

    #[cfg(feature = "cluster")]
    fn cluster_cycle_local_frontier(
        &self,
        supplied: InputFrontier,
        has_data: bool,
    ) -> Result<InputFrontier, DbError> {
        if self.last_broadcast == self.local_frontier {
            return Ok(supplied);
        }
        if has_data {
            return Err(DbError::InvalidOperation(format!(
                "managed CoreWindow '{}' received local input before its restored frontier was broadcast",
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
                "managed CoreWindow '{}' received data from an idle local channel",
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
        let peers = self.peer_channels.iter().map(|(&peer, channel)| {
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
        let merged = merge_input_frontier_iter(std::iter::once(local).chain(peers), i64::MIN);
        self.validate_frontier(self.effective_frontier, merged, "effective")?;
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
    ) -> Result<EowcClusterInputPlan, DbError> {
        let window = self.state.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' is not initialized",
                self.op_name
            ))
        })?;
        let mut local_batches = Vec::new();
        let mut remote_data = BTreeMap::<u64, Vec<ShuffleMessage>>::new();
        for batch in batches.into_iter().filter(|batch| batch.num_rows() != 0) {
            let row_vnodes = crate::operator::sql_query::hash_rows_to_vnodes(
                &batch,
                window.num_group_cols(),
                u32::from(self.key_group_count),
            )
            .map_err(|error| {
                crate::operator::shuffle_routing_error(
                    &format!("managed CoreWindow '{}' routing", self.op_name),
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
                    &format!("managed CoreWindow '{}' routing", self.op_name),
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
                "managed CoreWindow '{}' routed data outside its peer roster",
                self.op_name
            )));
        }
        let effective_frontier = self.effective_cluster_frontier(local_frontier, None)?;
        Ok(EowcClusterInputPlan {
            local_batches,
            outbound,
            local_frontier,
            effective_frontier,
        })
    }

    #[cfg(feature = "cluster")]
    fn remote_replay_error(&self, error: DbError) -> DbError {
        if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
            error
        } else {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' ordered shuffle replay requires recovery: {error}",
                self.op_name
            ))
        }
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
    fn drain_remote_event(
        &mut self,
        assignment: &VnodeAssignmentSnapshot,
        self_id: NodeId,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let peer = self.next_remote_peer().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' remote event accounting is inconsistent",
                self.op_name
            ))
        })?;
        let event = self.peer_channels[&peer]
            .events
            .front()
            .cloned()
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' selected an empty peer queue",
                    self.op_name
                ))
            })?;
        if event.assignment_version != assignment.version() {
            return Err(self.remote_replay_error(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' replay crossed its assignment boundary",
                self.op_name
            ))));
        }
        let channel = &self.peer_channels[&peer];
        let (local_batches, applied) = match &event.payload {
            EowcRemoteEventPayload::Data(batch) => {
                let plan = laminar_core::shuffle::route_checkpointed_batch(
                    batch.retained.batch(),
                    &batch.row_vnodes,
                    assignment,
                    self_id,
                )
                .map_err(|error| {
                    self.remote_replay_error(crate::operator::shuffle_routing_error(
                        &format!("managed CoreWindow '{}' queued routing", self.op_name),
                        &error,
                    ))
                })?;
                if !plan.remote.is_empty() {
                    return Err(self.remote_replay_error(DbError::Checkpoint(format!(
                        "managed CoreWindow '{}' queued data is no longer locally owned",
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
            EowcRemoteEventPayload::Frontier(frontier) => {
                self.validate_frontier(channel.applied, *frontier, "remote applied")
                    .map_err(|error| self.remote_replay_error(error))?;
                (Vec::new(), *frontier)
            }
        };
        let pending = channel.events.len() > 1;
        let effective = self
            .effective_cluster_frontier(self.local_frontier, Some((peer, applied, pending)))
            .map_err(|error| self.remote_replay_error(error))?;
        let output = {
            let window = self.state.as_mut().ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' replay targeted uninitialized state",
                    self.op_name
                ))
            })?;
            Self::apply_routed_and_close(
                window,
                &local_batches,
                Self::frontier_watermark(effective),
                &self.op_name,
            )
            .map_err(|error| self.remote_replay_error(error))?
        };
        let released = event.payload_bytes();
        let channel = self
            .peer_channels
            .get_mut(&peer)
            .expect("planned CoreWindow peer channel");
        channel
            .events
            .pop_front()
            .expect("planned CoreWindow remote event");
        if matches!(event.payload, EowcRemoteEventPayload::Frontier(_)) {
            channel.applied = applied;
        }
        self.queued_payload_bytes = self
            .queued_payload_bytes
            .checked_sub(released)
            .expect("CoreWindow queue accounting was prevalidated");
        self.queued_remote_events = self
            .queued_remote_events
            .checked_sub(1)
            .expect("CoreWindow event accounting was prevalidated");
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
            .expect("CoreWindow send plan must be installed before it starts");
        debug_assert!(pending.send.is_none());
        let outbound = pending
            .outbound
            .take()
            .expect("idle CoreWindow send plan must retain its outbound cut");
        let sender = Arc::clone(&config.sender);
        let wake = config.receiver.work_ready_notify();
        let context = format!("managed CoreWindow '{}' shuffle", self.op_name);
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
    fn outbound_finalize_error(&self, error: DbError) -> DbError {
        if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
            error
        } else {
            DbError::ShufflePartialSend(format!(
                "managed CoreWindow '{}' failed after outbound shuffle admission: {error}",
                self.op_name
            ))
        }
    }

    #[cfg(feature = "cluster")]
    async fn finish_pending_cluster_input(&mut self) -> Result<PendingEowcCompletion, DbError> {
        let finished = self
            .pending_cluster_input
            .as_ref()
            .and_then(|pending| pending.send.as_ref())
            .is_some_and(tokio::task::JoinHandle::is_finished);
        if !finished {
            return Ok(PendingEowcCompletion::Waiting);
        }
        let mut pending = self
            .pending_cluster_input
            .take()
            .expect("finished CoreWindow send plan");
        let send = pending.send.take().expect("pending CoreWindow send task");
        let (result, outbound) = send.await.map_err(|error| {
            DbError::ShufflePartialSend(format!(
                "managed CoreWindow '{}' send task ended without a delivery outcome: {error}",
                self.op_name
            ))
        })?;
        if let Err(error) = result {
            if error.is_shuffle_not_ready() {
                pending.outbound = Some(outbound.ok_or_else(|| {
                    DbError::ShufflePartialSend(format!(
                        "managed CoreWindow '{}' safe send failure lost its retry plan",
                        self.op_name
                    ))
                })?);
                self.pending_cluster_input = Some(pending);
                return Ok(PendingEowcCompletion::RetryLater);
            }
            return Err(error);
        }
        debug_assert!(outbound.is_none());

        let effective = self
            .effective_cluster_frontier(pending.local_frontier, None)
            .map_err(|error| self.outbound_finalize_error(error))?;
        let output = {
            let window = self.state.as_mut().ok_or_else(|| {
                DbError::ShufflePartialSend(format!(
                    "managed CoreWindow '{}' is not initialized",
                    self.op_name
                ))
            })?;
            Self::apply_routed_and_close(
                window,
                &pending.local_batches,
                Self::frontier_watermark(effective),
                &self.op_name,
            )
        }
        .map_err(|error| self.outbound_finalize_error(error))?;
        self.local_frontier = pending.local_frontier;
        self.last_broadcast = pending.local_frontier;
        self.effective_frontier = effective;
        Ok(PendingEowcCompletion::Applied(output))
    }

    #[cfg(feature = "cluster")]
    async fn process_cluster(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontier: InputFrontier,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let (config, assignment, peers) = self.active_cluster_scope()?;
        match self.finish_pending_cluster_input().await? {
            PendingEowcCompletion::Applied(output) => return Ok(output),
            PendingEowcCompletion::Waiting | PendingEowcCompletion::RetryLater => {}
        }
        if self.queued_remote_events != 0 {
            if inputs.iter().any(|batches| !batches.is_empty()) {
                return Err(DbError::InvalidOperation(format!(
                    "managed CoreWindow '{}' received local input while ordered shuffle replay was pending",
                    self.op_name
                )));
            }
            return self.drain_remote_event(&assignment, config.self_id);
        }
        if self.pending_cluster_input.is_some() {
            if inputs.iter().any(|batches| !batches.is_empty()) {
                return Err(DbError::InvalidOperation(format!(
                    "managed CoreWindow '{}' received local input while a shuffle send was pending",
                    self.op_name
                )));
            }
            if self
                .pending_cluster_input
                .as_ref()
                .is_some_and(|pending| pending.send.is_none())
            {
                self.start_pending_cluster_send(&config, assignment.version());
            }
            return Ok(Vec::new());
        }
        let input_batches = inputs.first().map_or(&[][..], Vec::as_slice);
        let has_data = input_batches.iter().any(|batch| batch.num_rows() != 0);
        // A restored/transitioned topology deliberately leaves its exact local cut
        // unbroadcast, which makes `wants_input` hold graph-buffered rows. Do not let the
        // concurrently observed live source frontier leap over that cut during this
        // frontier-only bootstrap cycle. Once the cut is acknowledged, normal node-local
        // frontier advancement resumes on the next cycle.
        let frontier = self.cluster_cycle_local_frontier(frontier, has_data)?;
        let local_frontier = self.normalized_local_frontier(frontier, has_data)?;
        let watermark = Self::frontier_watermark(local_frontier);
        let pre_aggregate = {
            let window = self.state.as_mut().ok_or_else(|| {
                DbError::Pipeline(format!(
                    "managed CoreWindow '{}' is not initialized",
                    self.op_name
                ))
            })?;
            Self::pre_aggregate(
                window,
                input_batches,
                watermark,
                &self.op_name,
                &self.ctx,
                &self.task_ctx,
                true,
            )
            .await?
        };
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
                    context: format!("managed CoreWindow '{}' pending shuffle send", self.op_name),
                    accounted_bytes: total,
                    limit_bytes: self.max_managed_state_bytes,
                });
            }
            let EowcClusterInputPlan {
                local_batches,
                outbound,
                local_frontier,
                effective_frontier: _,
            } = plan;
            self.pending_cluster_input = Some(PendingEowcClusterInput {
                local_batches,
                outbound: Some(outbound),
                local_frontier,
                send: None,
                accounted_bytes,
            });
            self.start_pending_cluster_send(&config, assignment.version());
            return Ok(Vec::new());
        }
        let output = {
            let window = self.state.as_mut().expect("initialized CoreWindow state");
            Self::apply_routed_and_close(
                window,
                &plan.local_batches,
                Self::frontier_watermark(plan.effective_frontier),
                &self.op_name,
            )
        }?;
        self.local_frontier = plan.local_frontier;
        self.last_broadcast = plan.local_frontier;
        self.effective_frontier = plan.effective_frontier;
        Ok(output)
    }

    #[cfg(feature = "cluster")]
    fn capture_cluster_checkpoint(
        &self,
        max_capture_bytes: usize,
    ) -> Result<Option<CapturedEowcCluster>, DbError> {
        let Some(cluster) = self.cluster_scope.as_ref() else {
            return Ok(None);
        };
        let (_, assignment, peers) = self.active_cluster_scope()?;
        if self.pending_cluster_input.is_some()
            || self.last_broadcast != self.local_frontier
            || self.peer_channels.len() != peers.len()
            || self
                .remote_peer_cursor
                .is_some_and(|peer| peers.binary_search(&peer).is_err())
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' cluster frontier topology is not at a checkpoint boundary",
                self.op_name
            )));
        }
        let effective = self.effective_cluster_frontier(self.local_frontier, None)?;
        let window = self.state.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' checkpoint targeted uninitialized state",
                self.op_name
            ))
        })?;
        if effective.watermark != self.effective_frontier.watermark
            || (self
                .peer_channels
                .values()
                .all(|channel| channel.events.is_empty())
                && effective != self.effective_frontier)
            || window.high_watermark_ms() != Self::frontier_watermark(self.effective_frontier)
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' applied cluster frontier is inconsistent",
                self.op_name
            )));
        }

        let mut queued_payload_bytes = 0usize;
        let mut queued_remote_events = 0usize;
        let mut event_capacity_bytes = 0usize;
        let mut requested_retained_bytes = self
            .peer_channels
            .len()
            .checked_mul(std::mem::size_of::<CapturedEowcChannel>())
            .and_then(|bytes| {
                bytes.checked_add(usize::from(bytes != 0) * OPERATOR_CAPTURE_ALLOCATION_CHARGE)
            })
            .ok_or_else(|| self.accounting_error())?;
        for (&peer, channel) in &self.peer_channels {
            if peers.binary_search(&peer).is_err() {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' checkpoint contains unknown peer {peer}",
                    self.op_name
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
            requested_retained_bytes = requested_retained_bytes
                .checked_add(
                    channel
                        .events
                        .len()
                        .checked_mul(std::mem::size_of::<CapturedEowcEvent>())
                        .and_then(|bytes| {
                            bytes.checked_add(
                                usize::from(bytes != 0) * OPERATOR_CAPTURE_ALLOCATION_CHARGE,
                            )
                        })
                        .ok_or_else(|| self.accounting_error())?,
                )
                .ok_or_else(|| self.accounting_error())?;
            let mut accepted = channel.applied;
            let mut previous_recovery = None;
            for event in &channel.events {
                if event.assignment_version != assignment.version()
                    || event.recovery_gen > cluster.receiver.recovery_gen()
                    || previous_recovery.is_some_and(|previous| event.recovery_gen < previous)
                {
                    return Err(DbError::Checkpoint(format!(
                        "managed CoreWindow '{}' peer {peer} queue crosses its assignment",
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
                match &event.payload {
                    EowcRemoteEventPayload::Data(batch) => {
                        if accepted.idle
                            || batch.retained.peer() != Some(peer)
                            || batch.retained.assignment_version() != Some(event.assignment_version)
                            || batch.retained.recovery_gen() != Some(event.recovery_gen)
                        {
                            return Err(DbError::Checkpoint(format!(
                                "managed CoreWindow '{}' peer {peer} queue has invalid data scope",
                                self.op_name
                            )));
                        }
                        requested_retained_bytes = requested_retained_bytes
                            .checked_add(
                                batch
                                    .retained
                                    .heap_bytes()
                                    .and_then(|bytes| bytes.checked_add(RETAINED_BATCH_ARC_CHARGE))
                                    .ok_or_else(|| self.accounting_error())?,
                            )
                            .ok_or_else(|| self.accounting_error())?;
                    }
                    EowcRemoteEventPayload::Frontier(frontier) => {
                        self.validate_frontier(accepted, *frontier, "accepted remote")?;
                        if accepted.idle && !frontier.idle {
                            let normalized = InputFrontier {
                                watermark: Self::max_watermark(
                                    frontier.watermark,
                                    self.effective_frontier.watermark,
                                ),
                                idle: false,
                            };
                            if *frontier != normalized {
                                return Err(DbError::Checkpoint(format!(
                                    "managed CoreWindow '{}' peer {peer} revival is below the effective frontier",
                                    self.op_name
                                )));
                            }
                        }
                        accepted = *frontier;
                    }
                }
            }
            if accepted != channel.accepted {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' peer {peer} accepted frontier is not derivable from its queue",
                    self.op_name
                )));
            }
        }
        if queued_payload_bytes != self.queued_payload_bytes
            || queued_remote_events != self.queued_remote_events
            || event_capacity_bytes != self.queued_event_capacity_bytes
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' channel accounting is inconsistent",
                self.op_name
            )));
        }
        if requested_retained_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' channel capture requires {requested_retained_bytes} bytes; capture headroom is {max_capture_bytes} bytes",
                self.op_name
            )));
        }

        let mut channels = Vec::new();
        channels
            .try_reserve_exact(self.peer_channels.len())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' could not reserve checkpoint channels: {error}",
                    self.op_name
                ))
            })?;
        for (&peer, channel) in &self.peer_channels {
            let mut events = Vec::new();
            events
                .try_reserve_exact(channel.events.len())
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "managed CoreWindow '{}' could not reserve peer {peer} checkpoint events: {error}",
                        self.op_name
                    ))
                })?;
            events.extend(channel.events.iter().map(|event| match &event.payload {
                EowcRemoteEventPayload::Data(batch) => CapturedEowcEvent::Data {
                    recovery_gen: event.recovery_gen,
                    retained: Arc::clone(&batch.retained),
                },
                EowcRemoteEventPayload::Frontier(frontier) => CapturedEowcEvent::Frontier {
                    recovery_gen: event.recovery_gen,
                    frontier: *frontier,
                },
            }));
            channels.push(CapturedEowcChannel {
                peer,
                applied: channel.applied,
                events,
            });
        }
        let retained_bytes = channels
            .capacity()
            .checked_mul(std::mem::size_of::<CapturedEowcChannel>())
            .and_then(|bytes| {
                bytes.checked_add(usize::from(bytes != 0) * OPERATOR_CAPTURE_ALLOCATION_CHARGE)
            })
            .and_then(|mut bytes| {
                for channel in &channels {
                    let event_bytes = channel
                        .events
                        .capacity()
                        .checked_mul(std::mem::size_of::<CapturedEowcEvent>())
                        .and_then(|event_bytes| {
                            event_bytes.checked_add(
                                usize::from(event_bytes != 0) * OPERATOR_CAPTURE_ALLOCATION_CHARGE,
                            )
                        })?;
                    bytes = bytes.checked_add(event_bytes)?;
                    for event in &channel.events {
                        if let CapturedEowcEvent::Data { retained, .. } = event {
                            bytes = bytes
                                .checked_add(retained.heap_bytes()?)?
                                .checked_add(RETAINED_BATCH_ARC_CHARGE)?;
                        }
                    }
                }
                Some(bytes)
            })
            .ok_or_else(|| self.accounting_error())?;
        if retained_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' channel capture retains {retained_bytes} bytes; capture headroom is {max_capture_bytes} bytes",
                self.op_name
            )));
        }
        Ok(Some(CapturedEowcCluster {
            assignment_version: assignment.version(),
            owner_map_digest: self
                .cluster_assignment_digest
                .expect("validated CoreWindow assignment digest"),
            self_id: cluster.self_id.0,
            local_frontier: self.local_frontier,
            effective_frontier: self.effective_frontier,
            remote_peer_cursor: self.remote_peer_cursor,
            channels,
            retained_bytes,
        }))
    }

    fn capture_operator_checkpoint(
        &self,
        max_capture_bytes: usize,
    ) -> Result<Option<EowcOperatorCheckpointCapture>, DbError> {
        let Some(window) = self.state.as_ref() else {
            return Ok(None);
        };
        let base_bytes = std::mem::size_of::<EowcOperatorCheckpointCapture>();
        #[cfg(feature = "cluster")]
        let cluster = self.capture_cluster_checkpoint(
            max_capture_bytes.checked_sub(base_bytes).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "EOWC '{}' checkpoint metadata exceeds its {max_capture_bytes}-byte headroom",
                    self.op_name
                ))
            })?,
        )?;
        #[cfg(feature = "cluster")]
        let cluster_bytes = cluster.as_ref().map_or(0, |cluster| cluster.retained_bytes);
        #[cfg(not(feature = "cluster"))]
        let cluster_bytes = 0;
        let retained_bytes = base_bytes
            .checked_add(cluster_bytes)
            .and_then(|bytes| u64::try_from(bytes).ok())
            .filter(|bytes| *bytes <= u64::try_from(max_capture_bytes).unwrap_or(u64::MAX))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "EOWC '{}' checkpoint capture exceeds its {max_capture_bytes}-byte headroom",
                    self.op_name
                ))
            })?;
        Ok(Some(EowcOperatorCheckpointCapture {
            high_watermark_ms: window.high_watermark_ms(),
            #[cfg(feature = "cluster")]
            cluster,
            retained_bytes,
        }))
    }

    #[cfg(feature = "cluster")]
    fn decode_cluster_checkpoint(
        &self,
        checkpoint: EowcClusterCheckpoint,
        high_watermark_ms: i64,
    ) -> Result<DecodedEowcCluster, DbError> {
        let (config, assignment, peers) = self.active_cluster_scope()?;
        if checkpoint.assignment_version != assignment.version()
            || checkpoint.owner_map_digest != self.cluster_assignment_digest.unwrap_or([0; 32])
            || checkpoint.self_id != config.self_id.0
            || checkpoint.channels.len() != peers.len()
            || checkpoint
                .remote_peer_cursor
                .is_some_and(|peer| peers.binary_search(&peer).is_err())
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' cluster checkpoint does not match assignment {}",
                self.op_name,
                assignment.version()
            )));
        }
        let local_frontier: InputFrontier = checkpoint.local_frontier.into();
        let effective_frontier: InputFrontier = checkpoint.effective_frontier.into();
        if local_frontier.watermark == Some(i64::MIN)
            || effective_frontier.watermark == Some(i64::MIN)
            || Self::frontier_watermark(effective_frontier) != high_watermark_ms
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' checkpoint has an invalid distributed frontier",
                self.op_name
            )));
        }
        let has_data = checkpoint.channels.iter().any(|channel| {
            channel
                .events
                .iter()
                .any(|event| matches!(event, EowcCheckpointEvent::Data { .. }))
        });
        let mut reader = if has_data {
            Some(
                StreamReader::try_new(std::io::Cursor::new(checkpoint.data_ipc.as_slice()), None)
                    .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "managed CoreWindow '{}' channel IPC restore: {error}",
                        self.op_name
                    ))
                })?,
            )
        } else {
            if !checkpoint.data_ipc.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' checkpoint has IPC without data events",
                    self.op_name
                )));
            }
            None
        };
        let restore_base = self.checked_live_state_bytes()?;
        self.ensure_cluster_restore_budget(restore_base, 0, 0)?;
        let mut peer_channels = BTreeMap::new();
        let mut queued_payload_bytes = 0usize;
        let mut queued_event_capacity_bytes = 0usize;
        let mut queued_remote_events = 0usize;
        for (expected_peer, archived) in peers.iter().zip(checkpoint.channels) {
            let EowcCheckpointChannel {
                peer,
                applied,
                events: archived_events,
            } = archived;
            if peer != *expected_peer {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' checkpoint peers are not canonical",
                    self.op_name
                )));
            }
            let applied: InputFrontier = applied.into();
            if applied.watermark == Some(i64::MIN) {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' peer {peer} has an invalid applied frontier",
                    self.op_name
                )));
            }
            if !applied.idle {
                self.validate_frontier(effective_frontier, applied, "restored peer applied")
                    .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            }
            let minimum_capacity = archived_events
                .len()
                .checked_mul(REMOTE_EVENT_CHARGE)
                .and_then(|bytes| queued_event_capacity_bytes.checked_add(bytes))
                .ok_or_else(|| self.accounting_error())?;
            self.ensure_cluster_restore_budget(
                restore_base,
                queued_payload_bytes,
                minimum_capacity,
            )?;
            let mut events = VecDeque::new();
            events
                .try_reserve_exact(archived_events.len())
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "managed CoreWindow '{}' peer {peer} queue reservation: {error}",
                        self.op_name
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
                queued_payload_bytes,
                queued_event_capacity_bytes,
            )?;
            let mut accepted = applied;
            let mut previous_recovery = None;
            for archived_event in archived_events {
                let event = match archived_event {
                    EowcCheckpointEvent::Data {
                        recovery_gen,
                        routed_vnodes,
                        row_count,
                    } => {
                        if row_count == 0
                            || row_count
                                > u64::try_from(laminar_core::shuffle::ROUTE_MAX_BATCH_ROWS)
                                    .unwrap_or(u64::MAX)
                            || recovery_gen > config.receiver.recovery_gen()
                            || previous_recovery.is_some_and(|previous| recovery_gen < previous)
                        {
                            return Err(DbError::Checkpoint(format!(
                                "managed CoreWindow '{}' peer {peer} data is outside its channel limits",
                                self.op_name
                            )));
                        }
                        previous_recovery = Some(recovery_gen);
                        let batch = match reader.as_mut().and_then(Iterator::next) {
                            Some(Ok(batch)) => batch,
                            Some(Err(error)) => {
                                return Err(DbError::Checkpoint(format!(
                                    "managed CoreWindow '{}' channel IPC restore: {error}",
                                    self.op_name
                                )));
                            }
                            None => {
                                return Err(DbError::Checkpoint(format!(
                                    "managed CoreWindow '{}' channel IPC has fewer batches than data events",
                                    self.op_name
                                )));
                            }
                        };
                        if u64::try_from(batch.num_rows()).ok() != Some(row_count) {
                            return Err(DbError::Checkpoint(format!(
                                "managed CoreWindow '{}' peer {peer} restored row count differs from its event",
                                self.op_name
                            )));
                        }
                        let retained = crate::operator::RetainedBatch::restored_channel(
                            batch,
                            peer,
                            checkpoint.assignment_version,
                            recovery_gen,
                            routed_vnodes.into(),
                        );
                        let batch = self
                            .build_queued_batch(retained, accepted, &assignment, config.self_id)
                            .map_err(|error| {
                                DbError::Checkpoint(format!(
                                    "managed CoreWindow '{}' peer {peer} restored data: {error}",
                                    self.op_name
                                ))
                            })?;
                        EowcRemoteEvent {
                            assignment_version: checkpoint.assignment_version,
                            recovery_gen,
                            payload: EowcRemoteEventPayload::Data(batch),
                        }
                    }
                    EowcCheckpointEvent::Frontier {
                        recovery_gen,
                        frontier,
                    } => {
                        let frontier: InputFrontier = frontier.into();
                        if frontier.watermark == Some(i64::MIN)
                            || recovery_gen > config.receiver.recovery_gen()
                            || previous_recovery.is_some_and(|previous| recovery_gen < previous)
                        {
                            return Err(DbError::Checkpoint(format!(
                                "managed CoreWindow '{}' peer {peer} frontier is outside its channel limits",
                                self.op_name
                            )));
                        }
                        previous_recovery = Some(recovery_gen);
                        self.validate_frontier(accepted, frontier, "restored remote")
                            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
                        if accepted.idle && !frontier.idle {
                            let normalized = InputFrontier {
                                watermark: Self::max_watermark(
                                    frontier.watermark,
                                    effective_frontier.watermark,
                                ),
                                idle: false,
                            };
                            if frontier != normalized {
                                return Err(DbError::Checkpoint(format!(
                                    "managed CoreWindow '{}' peer {peer} restored revival is below the effective frontier",
                                    self.op_name
                                )));
                            }
                        }
                        accepted = frontier;
                        EowcRemoteEvent {
                            assignment_version: checkpoint.assignment_version,
                            recovery_gen,
                            payload: EowcRemoteEventPayload::Frontier(frontier),
                        }
                    }
                };
                queued_payload_bytes = queued_payload_bytes
                    .checked_add(event.payload_bytes())
                    .ok_or_else(|| self.accounting_error())?;
                queued_remote_events = queued_remote_events
                    .checked_add(1)
                    .ok_or_else(|| self.accounting_error())?;
                self.ensure_cluster_restore_budget(
                    restore_base,
                    queued_payload_bytes,
                    queued_event_capacity_bytes,
                )?;
                events.push_back(event);
            }
            peer_channels.insert(
                peer,
                EowcPeerChannel {
                    applied,
                    accepted,
                    events,
                },
            );
        }
        if let Some(reader) = reader.as_mut() {
            match reader.next() {
                None => {}
                Some(Ok(_)) => {
                    return Err(DbError::Checkpoint(format!(
                        "managed CoreWindow '{}' channel IPC has more batches than data events",
                        self.op_name
                    )));
                }
                Some(Err(error)) => {
                    return Err(DbError::Checkpoint(format!(
                        "managed CoreWindow '{}' trailing channel IPC: {error}",
                        self.op_name
                    )));
                }
            }
        }
        let merged = merge_input_frontier_iter(
            std::iter::once(local_frontier).chain(peer_channels.values().map(|channel| {
                let mut frontier = channel.applied;
                if !channel.events.is_empty() {
                    frontier.idle = false;
                    frontier.watermark =
                        Self::max_watermark(frontier.watermark, effective_frontier.watermark);
                }
                frontier
            })),
            i64::MIN,
        );
        self.validate_frontier(effective_frontier, merged, "restored effective")
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        if merged.watermark != effective_frontier.watermark
            || (peer_channels
                .values()
                .all(|channel| channel.events.is_empty())
                && merged != effective_frontier)
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' restored channel frontier does not match its saved cut",
                self.op_name
            )));
        }
        Ok(DecodedEowcCluster {
            local_frontier,
            effective_frontier,
            remote_peer_cursor: checkpoint.remote_peer_cursor,
            peer_channels,
            queued_payload_bytes,
            queued_event_capacity_bytes,
            queued_remote_events,
        })
    }

    fn encode_vnode_checkpoint(
        checkpoint: &CoreWindowVnodeCheckpoint,
        op_name: &str,
        vnode: u32,
        max_encoded_bytes: usize,
    ) -> Result<EncodedStateFrame, DbError> {
        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(max_encoded_bytes),
        );
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(checkpoint, writer)
            .map(|bytes| EncodedStateFrame::from_vec(bytes.into_inner().into_vec()))
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "CoreWindow '{op_name}' vnode {vnode} checkpoint exceeded its {max_encoded_bytes}-byte archive limit: {error}"
                ))
            })
    }

    #[cfg(feature = "cluster")]
    fn validate_drained_transition_cut(
        &self,
        assignment: &VnodeAssignmentSnapshot,
        window: &CoreWindowState,
        self_id: NodeId,
    ) -> Result<(), DbError> {
        let expected_peers = Self::remote_owner_peers(assignment, self_id);
        if self.cluster_peers.as_ref() != expected_peers.as_slice()
            || self.peer_channels.len() != expected_peers.len()
            || !self
                .peer_channels
                .keys()
                .copied()
                .eq(expected_peers.iter().copied())
            || self
                .remote_peer_cursor
                .is_some_and(|peer| expected_peers.binary_search(&peer).is_err())
            || self.pending_cluster_input.is_some()
            || self.last_broadcast != self.local_frontier
            || self.queued_payload_bytes != 0
            || self.queued_remote_events != 0
            || self.local_frontier.watermark == Some(i64::MIN)
            || self.effective_frontier.watermark == Some(i64::MIN)
            || self.cluster_assignment_digest != Some(self.owner_map_digest(assignment))
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' transition requires a drained frontier and channel cut",
                self.op_name
            )));
        }
        let mut event_capacity_bytes = 0usize;
        for channel in self.peer_channels.values() {
            if channel.applied.watermark == Some(i64::MIN)
                || channel.accepted != channel.applied
                || !channel.events.is_empty()
            {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' transition found retained ordered channel state",
                    self.op_name
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
        let merged = merge_input_frontier_iter(
            std::iter::once(self.local_frontier)
                .chain(self.peer_channels.values().map(|channel| channel.applied)),
            i64::MIN,
        );
        if event_capacity_bytes != self.queued_event_capacity_bytes
            || merged != self.effective_frontier
            || window.high_watermark_ms() != Self::frontier_watermark(self.effective_frontier)
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' transition found inconsistent channel accounting or frontier",
                self.op_name
            )));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn portable_handoff_cut(
        &self,
        transition: &ManagedVnodeTransition<'_>,
    ) -> Result<Option<InputFrontier>, DbError> {
        let mut donors = BTreeSet::new();
        for restore in transition.restores {
            let predecessor_owner = match transition.mode {
                ManagedVnodeTransitionMode::Live => self
                    .cluster_assignment
                    .as_ref()
                    .and_then(|assignment| assignment.owners().get(restore.vnode as usize))
                    .copied(),
                ManagedVnodeTransitionMode::CheckpointBootstrap { predecessor_owners } => {
                    predecessor_owners.get(restore.vnode as usize).copied()
                }
            };
            if !transition.predecessor.contains(restore.participant_id)
                || predecessor_owner.map(|owner| owner.0) != Some(restore.participant_id)
            {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' vnode {} restore has invalid donor {}",
                    self.op_name, restore.vnode, restore.participant_id
                )));
            }
            donors.insert(restore.participant_id);
        }
        if donors.is_empty() {
            if transition.whole_restores.is_empty() {
                return Ok(None);
            }
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' has whole handoff frames without acquired vnodes",
                self.op_name
            )));
        }

        let predecessor_participants = transition.predecessor.participant_ids();
        let mut whole_donors = BTreeSet::new();
        let mut common = None;
        for restore in transition.whole_restores {
            if !whole_donors.insert(restore.participant_id)
                || restore.state.len() > self.max_managed_state_bytes
            {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' has an invalid whole frame for donor {}",
                    self.op_name, restore.participant_id
                )));
            }
            let checkpoint = with_aligned_checkpoint_bytes(restore.state, |state| {
                rkyv::from_bytes::<EowcOperatorCheckpoint, rkyv::rancor::Error>(state).map_err(
                    |error| {
                        DbError::Checkpoint(format!(
                            "managed CoreWindow '{}' donor {} whole checkpoint: {error}",
                            self.op_name, restore.participant_id
                        ))
                    },
                )
            })?;
            if checkpoint.version != OPERATOR_CHECKPOINT_VERSION {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' donor {} uses unsupported checkpoint version {}",
                    self.op_name, restore.participant_id, checkpoint.version
                )));
            }
            let cluster = checkpoint.cluster.ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' donor {} whole checkpoint is not from cluster mode",
                    self.op_name, restore.participant_id
                ))
            })?;
            let local: InputFrontier = cluster.local_frontier.into();
            let effective: InputFrontier = cluster.effective_frontier.into();
            let expected_peers = predecessor_participants
                .iter()
                .copied()
                .filter(|peer| *peer != restore.participant_id)
                .collect::<Vec<_>>();
            if cluster.assignment_version != transition.predecessor.assignment_version
                || cluster.owner_map_digest != transition.predecessor.assignment_digest
                || cluster.self_id != restore.participant_id
                || local.watermark == Some(i64::MIN)
                || effective.watermark == Some(i64::MIN)
                || checkpoint.high_watermark_ms != Self::frontier_watermark(effective)
                || cluster.channels.len() != expected_peers.len()
                || !cluster.data_ipc.is_empty()
                || cluster
                    .remote_peer_cursor
                    .is_some_and(|peer| expected_peers.binary_search(&peer).is_err())
            {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' donor {} whole checkpoint is not a portable predecessor cut",
                    self.op_name, restore.participant_id
                )));
            }
            let mut applied = Vec::with_capacity(cluster.channels.len());
            for (expected_peer, channel) in expected_peers.iter().zip(cluster.channels) {
                let frontier: InputFrontier = channel.applied.into();
                if channel.peer != *expected_peer
                    || frontier.watermark == Some(i64::MIN)
                    || !channel.events.is_empty()
                {
                    return Err(DbError::Checkpoint(format!(
                        "managed CoreWindow '{}' donor {} has retained ordered channel state",
                        self.op_name, restore.participant_id
                    )));
                }
                applied.push(frontier);
            }
            let merged = merge_input_frontier_iter(std::iter::once(local).chain(applied), i64::MIN);
            if merged != effective {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' donor {} frontiers do not form one handoff cut",
                    self.op_name, restore.participant_id
                )));
            }
            if common.is_some_and(|expected| expected != effective) {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' donor whole checkpoints disagree on the handoff cut",
                    self.op_name
                )));
            }
            common = Some(effective);
        }
        if whole_donors != donors {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' whole checkpoints do not exactly cover acquired vnode donors",
                self.op_name
            )));
        }
        common.map(Some).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' acquired vnodes without a portable whole cut",
                self.op_name
            ))
        })
    }

    #[cfg(feature = "cluster")]
    fn prepare_transition_image(
        &self,
        transition: &ManagedVnodeTransition<'_>,
    ) -> Result<PreparedEowcTransition, DbError> {
        let config = self.cluster_scope.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' cannot transition without cluster ownership",
                self.op_name
            ))
        })?;
        let window = self.state.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow transition targeted uninitialized operator '{}'",
                self.op_name
            ))
        })?;
        let installed = self.cluster_assignment.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' has no installed assignment",
                self.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let owners = assignment
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
                        "managed CoreWindow '{}' checkpoint bootstrap has an invalid predecessor owner map",
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
        let installed_owners = installed
            .owners()
            .iter()
            .map(|owner| owner.0)
            .collect::<Vec<_>>();
        let bootstrap_pristine = window.is_pristine_for_restore()
            && !self.whole_restore_applied
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
        if transition.target.vnode_count != u32::from(self.key_group_count)
            || transition.predecessor.vnode_count != u32::from(self.key_group_count)
            || transition.target.assignment_version != assignment.version()
            || !transition.target.matches_owner_map(&owners)
            || !version_edge_valid
            || config.sender.recovery_gen() != config.receiver.recovery_gen()
            || target_contains_self != transition.target.contains(config.self_id.0)
            || (target_contains_self && !active_transport)
            || (!target_contains_self && !inactive_transport)
            || if checkpoint_bootstrap {
                installed.version() != assignment.version()
                    || installed.owners() != assignment.owners()
                    || !bootstrap_pristine
            } else {
                transition.predecessor.assignment_version != installed.version()
                    || !transition.predecessor.matches_owner_map(&installed_owners)
                    || self.cluster_assignment_digest
                        != Some(transition.predecessor.assignment_digest)
            }
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' transition is outside its adjacent assignment",
                self.op_name
            )));
        }
        self.validate_drained_transition_cut(installed, window, config.self_id)?;

        let predecessor_owned = if !checkpoint_bootstrap
            && transition
                .predecessor
                .participant_incarnation(config.self_id.0)
                == Some(config.sender.incarnation())
        {
            installed
                .owners()
                .iter()
                .enumerate()
                .filter(|(_, owner)| **owner == config.self_id)
                .map(|(vnode, _)| {
                    u32::try_from(vnode).expect("CoreWindow vnode topology must fit u32")
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
            .map(|(vnode, _)| u32::try_from(vnode).expect("CoreWindow vnode topology must fit u32"))
            .collect::<Vec<_>>();
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
                "managed CoreWindow '{}' transition does not match its exact ownership delta",
                self.op_name
            )));
        }

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
            .map(|restore| checkpoint_alignment_copy_bytes(restore.state))
            .chain(
                transition
                    .whole_restores
                    .iter()
                    .map(|restore| checkpoint_alignment_copy_bytes(restore.state)),
            )
            .max()
            .unwrap_or(0);
        let payload_phase_bytes = payload_bytes
            .checked_add(alignment_copy_bytes)
            .ok_or_else(|| self.accounting_error())?;
        if payload_phase_bytes > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("managed CoreWindow '{}' transition payload", self.op_name),
                accounted_bytes: payload_phase_bytes,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        let fresh_acquirer =
            !target_owned.is_empty() && (checkpoint_bootstrap || predecessor_owned.is_empty());
        let handoff_cut = self.portable_handoff_cut(transition)?;
        if !fresh_acquirer
            && handoff_cut.is_some_and(|frontier| frontier != self.effective_frontier)
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' donor cut does not match the retained owner frontier",
                self.op_name
            )));
        }
        let transition_frontier = if fresh_acquirer {
            handoff_cut.ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' fresh owner is missing its portable whole cut",
                    self.op_name
                ))
            })?
        } else {
            self.effective_frontier
        };
        let final_frontier_ms = Self::frontier_watermark(transition_frontier);

        let mut preflighted = Vec::new();
        preflighted
            .try_reserve_exact(transition.restores.len())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "CoreWindow '{}' could not reserve vnode restore metadata: {error}",
                    self.op_name
                ))
            })?;
        for restore in transition.restores {
            with_aligned_checkpoint_bytes(restore.state, |state| {
                window
                    .preflight_vnode_bytes(restore.vnode, transition.target.vnode_count, state)
                    .map(|_| ())
            })?;
            preflighted.push((restore.vnode, restore.state));
        }
        let owned_restores = preflighted.into_iter().map(|(vnode, bytes)| {
            let state = with_aligned_checkpoint_bytes(bytes, |state| {
                let state =
                    window.preflight_vnode_bytes(vnode, transition.target.vnode_count, state)?;
                rkyv::deserialize::<CoreWindowVnodeCheckpoint, rkyv::rancor::Error>(
                    state.checkpoint,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "CoreWindow '{}' vnode {vnode} transition deserialization: {error}",
                        self.op_name
                    ))
                })
            })?;
            Ok(crate::core_window_state::OwnedCoreWindowVnodeRestore { vnode, state })
        });
        let core = window.prepare_owned_vnode_transition(
            transition.target.vnode_count,
            final_frontier_ms,
            owned_restores,
            transition.revoked,
        )?;

        let target_peers = Self::remote_owner_peers(&assignment, config.self_id);
        let reset_topology = fresh_acquirer || target_owned.is_empty();
        let mut channels = BTreeMap::new();
        for &peer in &target_peers {
            let same_incarnation = transition.predecessor.participant_incarnation(peer)
                == transition.target.participant_incarnation(peer);
            let channel = if reset_topology || !same_incarnation {
                EowcPeerChannel {
                    applied: transition_frontier,
                    accepted: transition_frontier,
                    events: VecDeque::new(),
                }
            } else {
                self.peer_channels.get(&peer).map_or(
                    EowcPeerChannel {
                        applied: transition_frontier,
                        accepted: transition_frontier,
                        events: VecDeque::new(),
                    },
                    |channel| EowcPeerChannel {
                        applied: channel.applied,
                        accepted: channel.accepted,
                        events: VecDeque::new(),
                    },
                )
            };
            channels.insert(peer, channel);
        }
        let local_frontier = if reset_topology {
            transition_frontier
        } else if self.local_frontier.idle {
            InputFrontier {
                watermark: Self::max_watermark(
                    self.local_frontier.watermark,
                    transition_frontier.watermark,
                ),
                idle: true,
            }
        } else {
            self.local_frontier
        };
        let merged = merge_input_frontier_iter(
            std::iter::once(local_frontier).chain(channels.values().map(|channel| channel.applied)),
            i64::MIN,
        );
        self.validate_frontier(transition_frontier, merged, "transition target")?;
        if reset_topology && merged != transition_frontier {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' reset target channels do not form the transition cut",
                self.op_name
            )));
        }
        let last_broadcast = if !target_owned.is_empty()
            && (!target_peers.is_empty() || merged != transition_frontier)
        {
            InputFrontier::default()
        } else {
            local_frontier
        };
        let prepared = PreparedEowcTransition {
            core,
            topology: EowcTransitionTopology {
                assignment,
                assignment_digest: transition.target.assignment_digest,
                peers: target_peers.into(),
                channels,
                local_frontier,
                last_broadcast,
                effective_frontier: transition_frontier,
                remote_peer_cursor: None,
                queued_payload_bytes: 0,
                queued_event_capacity_bytes: 0,
                queued_remote_events: 0,
            },
        };
        let accounted = self
            .checked_live_state_bytes()?
            .checked_add(payload_bytes)
            .and_then(|bytes| bytes.checked_add(prepared.accounted_state_bytes()))
            .ok_or_else(|| self.accounting_error())?;
        if accounted > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("managed CoreWindow '{}' prepared transition", self.op_name),
                accounted_bytes: accounted,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        Ok(prepared)
    }
}

#[async_trait]
impl GraphOperator for EowcQueryOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        self.capability
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        if self.capability.managed_state != Some(ManagedStateContract::CoreWindowV1) {
            return None;
        }
        let window = self.state.as_ref()?;
        #[cfg(feature = "cluster")]
        let (prepared, retired) = {
            let staged = self
                .prepared_vnode_transition
                .as_ref()
                .map_or(0, PreparedEowcTransition::accounted_state_bytes);
            match self.vnode_transition_cleanup.as_ref() {
                Some(CoreWindowTransitionCleanup::Aborted(cleanup)) => {
                    (staged.saturating_add(cleanup.accounted_state_bytes()), 0)
                }
                Some(CoreWindowTransitionCleanup::Published(cleanup)) => {
                    (staged, cleanup.accounted_state_bytes())
                }
                None => (staged, 0),
            }
        };
        #[cfg(not(feature = "cluster"))]
        let (prepared, retired) = (0, 0);
        #[cfg(feature = "cluster")]
        let live = self.checked_live_state_bytes().unwrap_or(usize::MAX);
        #[cfg(not(feature = "cluster"))]
        let live = window.accounted_state_bytes();
        #[cfg(feature = "cluster")]
        let _ = window;
        Some(ManagedStateAccountingSnapshot {
            live,
            prepared,
            retired,
        })
    }

    fn set_managed_state_budget(&mut self, bytes: usize) {
        self.max_managed_state_bytes = bytes;
    }

    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        if self.state.is_none() {
            self.initialize().await?;
        }
        #[cfg(feature = "cluster")]
        {
            let accounted = self.checked_live_state_bytes()?;
            if accounted > self.max_managed_state_bytes {
                return Err(DbError::ManagedStateBudgetExceeded {
                    context: format!("managed CoreWindow '{}' topology", self.op_name),
                    accounted_bytes: accounted,
                    limit_bytes: self.max_managed_state_bytes,
                });
            }
        }
        Ok(())
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let watermark = watermarks.first().copied().unwrap_or(i64::MIN);
        let frontier = InputFrontier {
            watermark: (watermark != i64::MIN).then_some(watermark),
            idle: false,
        };
        self.process_with_frontiers(inputs, std::slice::from_ref(&frontier))
            .await
    }

    async fn process_with_frontiers(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: &[InputFrontier],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if frontiers.len() != 1 {
            return Err(DbError::InvalidOperation(format!(
                "EOWC query '{}' requires one input frontier",
                self.op_name
            )));
        }
        if self.state.is_none() {
            self.initialize().await?;
        }
        #[cfg(feature = "cluster")]
        if self.cluster_scope.is_some() {
            return self.process_cluster(inputs, frontiers[0]).await;
        }
        let input_batches = inputs.first().map_or(&[][..], Vec::as_slice);
        if frontiers[0].idle && input_batches.iter().any(|batch| batch.num_rows() != 0) {
            return Err(DbError::InvalidOperation(format!(
                "EOWC query '{}' received data from an idle input",
                self.op_name
            )));
        }
        let watermark = Self::frontier_watermark(frontiers[0]);
        let pre_aggregate = {
            let window = self.state.as_mut().expect("initialized CoreWindow state");
            Self::pre_aggregate(
                window,
                input_batches,
                watermark,
                &self.op_name,
                &self.ctx,
                &self.task_ctx,
                false,
            )
            .await?
        };
        let routed = pre_aggregate
            .into_iter()
            .map(|batch| (batch, None))
            .collect::<Vec<_>>();
        let output = {
            let window = self.state.as_mut().expect("initialized CoreWindow state");
            Self::apply_routed_and_close(window, &routed, watermark, &self.op_name)?
        };
        Ok(output)
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
        let retained_bytes = capture.retained_bytes;
        let max_managed_state_bytes = self.max_managed_state_bytes;
        Ok(Some(StateFrameCapture::deferred(
            retained_bytes,
            move |headroom| {
                let data = capture.encode(headroom.min(max_managed_state_bytes))?;
                Ok(EncodedStateFrame::from_vec(data))
            },
        )))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let window = self.state.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "EOWC whole restore for '{}' requires initialized state",
                self.op_name
            ))
        })?;
        #[cfg(feature = "cluster")]
        let cluster_pristine = self.local_frontier == InputFrontier::default()
            && self.pending_cluster_input.is_none()
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
        #[cfg(not(feature = "cluster"))]
        let cluster_pristine = true;
        if self.whole_restore_applied || !cluster_pristine || !window.is_pristine_for_restore() {
            return Err(DbError::Checkpoint(format!(
                "EOWC whole checkpoint for '{}' was restored more than once or after processing",
                self.op_name
            )));
        }
        let OperatorCheckpoint { data } = checkpoint;
        let restore_bytes = data
            .len()
            .checked_add(checkpoint_alignment_copy_bytes(&data))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "EOWC whole checkpoint for '{}' exceeds its restore limit",
                    self.op_name
                ))
            })?;
        if restore_bytes > self.max_managed_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "EOWC whole checkpoint for '{}' exceeds its restore limit",
                self.op_name
            )));
        }
        let checkpoint = with_aligned_checkpoint_bytes(&data, |data| {
            rkyv::from_bytes::<EowcOperatorCheckpoint, rkyv::rancor::Error>(data).map_err(|error| {
                DbError::Checkpoint(format!(
                    "EOWC whole checkpoint deserialization for '{}': {error}",
                    self.op_name
                ))
            })
        })?;
        drop(data);
        if checkpoint.version != OPERATOR_CHECKPOINT_VERSION {
            return Err(DbError::Checkpoint(format!(
                "EOWC whole checkpoint for '{}' has unsupported version {}",
                self.op_name, checkpoint.version
            )));
        }
        #[cfg(feature = "cluster")]
        let decoded_cluster = match (self.cluster_scope.is_some(), checkpoint.cluster) {
            (true, Some(cluster)) => {
                Some(self.decode_cluster_checkpoint(cluster, checkpoint.high_watermark_ms)?)
            }
            (false, None) => None,
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "EOWC '{}' checkpoint deployment mode does not match the restored operator",
                    self.op_name
                )));
            }
        };
        #[cfg(not(feature = "cluster"))]
        if checkpoint.cluster.is_some() {
            return Err(DbError::Checkpoint(format!(
                "EOWC '{}' checkpoint contains cluster channel state",
                self.op_name
            )));
        }
        let window = self.state.as_mut().expect("initialized CoreWindow state");
        window
            .restore_high_watermark_ms(checkpoint.high_watermark_ms)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "EOWC whole restore for '{}': {error}",
                    self.op_name
                ))
            })?;
        #[cfg(feature = "cluster")]
        if let Some(cluster) = decoded_cluster {
            self.local_frontier = cluster.local_frontier;
            self.last_broadcast = cluster.local_frontier;
            self.effective_frontier = cluster.effective_frontier;
            self.remote_peer_cursor = cluster.remote_peer_cursor;
            self.peer_channels = cluster.peer_channels;
            self.queued_payload_bytes = cluster.queued_payload_bytes;
            self.queued_event_capacity_bytes = cluster.queued_event_capacity_bytes;
            self.queued_remote_events = cluster.queued_remote_events;
        }
        self.whole_restore_applied = true;
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn wants_input(&self) -> bool {
        self.pending_cluster_input.is_none()
            && self.queued_remote_events == 0
            && self.last_broadcast == self.local_frontier
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_aligned_replay_pending(&self) -> bool {
        self.pending_cluster_input.is_some() || self.queued_remote_events != 0
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_drain_pending(&self) -> bool {
        self.pending_cluster_input.is_some() || self.last_broadcast != self.local_frontier
    }

    #[cfg(feature = "cluster")]
    fn deferred_work_is_runnable(&self) -> bool {
        self.queued_remote_events != 0 || self.last_broadcast != self.local_frontier
    }

    #[cfg(feature = "cluster")]
    fn advances_frontier_without_input(&self) -> bool {
        self.cluster_scope.is_some()
    }

    #[cfg(feature = "cluster")]
    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        if self.cluster_scope.is_none() {
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
        self.cluster_scope.as_ref()?;
        let mut frontier = self.effective_frontier;
        if self.pending_cluster_input.is_some() || self.queued_remote_events != 0 {
            frontier.idle = false;
        }
        Some(frontier)
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
                "managed CoreWindow '{}' rejected unknown shuffle stage '{stage}'",
                self.op_name
            )));
        }
        let (config, assignment, peers) = self.active_cluster_scope()?;
        let peer = batch.peer().ok_or_else(|| {
            DbError::ShuffleTerminal(format!(
                "managed CoreWindow '{}' received unscoped shuffle data",
                self.op_name
            ))
        })?;
        if peers.binary_search(&peer).is_err()
            || batch.assignment_version() != Some(assignment.version())
            || batch.recovery_gen() != Some(config.receiver.recovery_gen())
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' received data from peer {peer} outside assignment {} recovery {}",
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
            .expect("validated CoreWindow assignment");
        let recovery_gen = batch
            .retained
            .recovery_gen()
            .expect("validated CoreWindow recovery generation");
        self.peer_channels
            .get_mut(&peer)
            .expect("reserved CoreWindow peer channel")
            .events
            .push_back(EowcRemoteEvent {
                assignment_version,
                recovery_gen,
                payload: EowcRemoteEventPayload::Data(batch),
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
                "managed CoreWindow '{}' rejected unknown frontier stage '{stage}'",
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
                "managed CoreWindow '{}' received frontier from peer {peer} outside assignment {} recovery {}",
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
            .expect("reserved CoreWindow peer channel");
        channel.events.push_back(EowcRemoteEvent {
            assignment_version,
            recovery_gen,
            payload: EowcRemoteEventPayload::Frontier(frontier),
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
    ) -> Result<Option<Vec<crate::operator_graph::CapturedVnodeState>>, DbError> {
        if self.capability.managed_state != Some(ManagedStateContract::CoreWindowV1) {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow capture targeted unsupported operator '{}'",
                self.op_name
            )));
        }
        let Some(window) = self.state.as_mut() else {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow capture targeted uninitialized operator '{}'",
                self.op_name
            )));
        };
        let vnode_captures =
            window.capture_checkpoint_vnodes(required_vnodes, vnode_count, max_capture_bytes)?;
        let mut captured = Vec::with_capacity(vnode_captures.len());
        for (vnode, capture) in vnode_captures {
            let retained_bytes = u64::try_from(capture.retained_bytes()).unwrap_or(u64::MAX);
            let op_name = Arc::clone(&self.op_name);
            let state = StateFrameCapture::deferred(retained_bytes, move |max_encoded_bytes| {
                let checkpoint = capture.encode(max_encoded_bytes)?;
                let intermediate_bytes = checkpoint.retained_serialization_bytes()?;
                let archive_budget = max_encoded_bytes
                    .checked_sub(intermediate_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "CoreWindow '{op_name}' vnode {vnode} intermediate checkpoint exhausted its frame budget"
                        ))
                    })?;
                Self::encode_vnode_checkpoint(&checkpoint, &op_name, vnode, archive_budget)
            });
            captured.push(crate::operator_graph::CapturedVnodeState {
                vnode,
                state: Some(state),
            });
        }
        Ok(Some(captured))
    }

    fn restore_vnode(&mut self, vnode: u32, vnode_count: u32, state: &[u8]) -> Result<(), DbError> {
        if !self.whole_restore_applied {
            return Err(DbError::Checkpoint(format!(
                "CoreWindow '{}' vnode restore requires its whole watermark frame",
                self.op_name
            )));
        }
        let window = self.state.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow vnode restore targeted uninitialized operator '{}'",
                self.op_name
            ))
        })?;
        let restore_bytes = state
            .len()
            .checked_add(checkpoint_alignment_copy_bytes(state))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "CoreWindow '{}' vnode {vnode} restore accounting overflow",
                    self.op_name
                ))
            })?;
        if restore_bytes > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "managed CoreWindow '{}' vnode {vnode} restore",
                    self.op_name
                ),
                accounted_bytes: restore_bytes,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        let checkpoint = with_aligned_checkpoint_bytes(state, |state| {
            let checkpoint = window.preflight_vnode_bytes(vnode, vnode_count, state)?;
            rkyv::deserialize::<CoreWindowVnodeCheckpoint, rkyv::rancor::Error>(
                checkpoint.checkpoint,
            )
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "CoreWindow '{}' vnode {vnode} checkpoint deserialization: {error}",
                    self.op_name
                ))
            })
        })?;
        let restored_high_watermark_ms = window.high_watermark_ms();
        let window = self
            .state
            .as_mut()
            .expect("CoreWindow restore state was checked above");
        window
            .restore_vnode(vnode, vnode_count, checkpoint)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "CoreWindow '{}' vnode {vnode} restore: {error}",
                    self.op_name
                ))
            })?;
        window
            .restore_high_watermark_ms(restored_high_watermark_ms)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "CoreWindow '{}' vnode {vnode} frontier validation: {error}",
                    self.op_name
                ))
            })
    }

    #[cfg(feature = "cluster")]
    fn prepare_vnode_transition(
        &mut self,
        transition: ManagedVnodeTransition<'_>,
    ) -> Result<(), DbError> {
        if self.prepared_vnode_transition.is_some() || self.vnode_transition_cleanup.is_some() {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' already owns vnode transition state",
                self.op_name
            )));
        }
        if self.capability.managed_state != Some(ManagedStateContract::CoreWindowV1) {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow transition targeted unsupported operator '{}'",
                self.op_name
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
        assert!(
            self.vnode_transition_cleanup.is_none(),
            "managed CoreWindow cleanup must finish before abort"
        );
        self.vnode_transition_cleanup = Some(CoreWindowTransitionCleanup::Aborted(prepared));
    }

    #[cfg(feature = "cluster")]
    fn publish_vnode_transition(&mut self) {
        let PreparedEowcTransition { core, mut topology } = self
            .prepared_vnode_transition
            .take()
            .expect("managed CoreWindow transition must be prepared before publication");
        assert!(
            self.vnode_transition_cleanup.is_none(),
            "managed CoreWindow cleanup must finish before publication"
        );
        let window = self
            .state
            .as_mut()
            .expect("managed CoreWindow publication targeted uninitialized state");
        let core = window.publish_prepared_vnode_transition(core);
        std::mem::swap(
            self.cluster_assignment
                .as_mut()
                .expect("prepared CoreWindow transition has an installed assignment"),
            &mut topology.assignment,
        );
        std::mem::swap(
            self.cluster_assignment_digest
                .as_mut()
                .expect("prepared CoreWindow transition has an assignment digest"),
            &mut topology.assignment_digest,
        );
        std::mem::swap(&mut self.cluster_peers, &mut topology.peers);
        std::mem::swap(&mut self.peer_channels, &mut topology.channels);
        std::mem::swap(&mut self.local_frontier, &mut topology.local_frontier);
        std::mem::swap(&mut self.last_broadcast, &mut topology.last_broadcast);
        std::mem::swap(
            &mut self.effective_frontier,
            &mut topology.effective_frontier,
        );
        std::mem::swap(
            &mut self.remote_peer_cursor,
            &mut topology.remote_peer_cursor,
        );
        std::mem::swap(
            &mut self.queued_payload_bytes,
            &mut topology.queued_payload_bytes,
        );
        std::mem::swap(
            &mut self.queued_event_capacity_bytes,
            &mut topology.queued_event_capacity_bytes,
        );
        std::mem::swap(
            &mut self.queued_remote_events,
            &mut topology.queued_remote_events,
        );
        self.vnode_transition_cleanup = Some(CoreWindowTransitionCleanup::Published(
            RetiredEowcTransition { core, topology },
        ));
    }

    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {
        match self.vnode_transition_cleanup.take() {
            Some(CoreWindowTransitionCleanup::Aborted(prepared)) => drop(prepared),
            Some(CoreWindowTransitionCleanup::Published(retired)) => {
                CoreWindowState::finish_vnode_transition(retired.core);
                drop(retired.topology);
            }
            None => {}
        }
    }

    fn force_full_vnode_capture(&mut self) {
        if let Some(window) = self.state.as_mut() {
            window.force_full_vnode_capture();
        }
    }
}

#[cfg(test)]
mod core_tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use std::time::Duration;

    const AGG_SQL: &str = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol";

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("ts", DataType::Int64, false),
        ]))
    }

    fn test_batch(ts_values: Vec<i64>) -> RecordBatch {
        let n = ts_values.len();
        let symbols: Vec<&str> = (0..n)
            .map(|i| if i % 2 == 0 { "AAPL" } else { "GOOG" })
            .collect();
        #[allow(clippy::cast_precision_loss)]
        let prices: Vec<f64> = (0..n).map(|i| (i as f64 + 1.0) * 100.0).collect();
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(symbols)),
                Arc::new(Float64Array::from(prices)),
                Arc::new(Int64Array::from(ts_values)),
            ],
        )
        .unwrap()
    }

    fn aggregate_context() -> SessionContext {
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let empty = MemTable::try_new(test_schema(), vec![vec![]]).unwrap();
        ctx.register_table("trades", Arc::new(empty)).unwrap();
        ctx
    }

    fn test_window_config() -> WindowOperatorConfig {
        WindowOperatorConfig {
            window_type: laminar_sql::translator::WindowType::Tumbling,
            time_column: "ts".to_string(),
            size: Duration::from_secs(60),
            slide: None,
            gap: None,
            offset_ms: 0,
            allowed_lateness: Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        }
    }

    fn key_groups() -> KeyGroupCount {
        KeyGroupCount::try_from(8_u32).unwrap()
    }

    #[cfg(feature = "cluster")]
    async fn cluster_scope(owners: [u64; 8]) -> ClusterShuffleConfig {
        use laminar_core::cluster::control::LeaseDeadline;
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use laminar_core::state::VnodeRegistry;

        let registry = Arc::new(VnodeRegistry::new(8));
        registry.set_assignment(Arc::from(owners.map(NodeId)));
        let receiver = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(1, uuid::Uuid::from_u128(1)));
        let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
        receiver
            .install_process_lease_deadline(Arc::clone(&deadline))
            .unwrap();
        sender.install_process_lease_deadline(deadline).unwrap();
        let fence = test_assignment_fence(registry.assignment_version(), &owners);
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
    fn test_assignment_fence(
        assignment_version: u64,
        owners: &[u64; 8],
    ) -> CheckpointAssignmentFence {
        use laminar_core::checkpoint::CheckpointParticipant;

        let participants = owners
            .iter()
            .copied()
            .filter(|node_id| *node_id != 0)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|node_id| CheckpointParticipant {
                node_id,
                boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
            })
            .collect();
        CheckpointAssignmentFence::from_owner_map(assignment_version, owners, participants).unwrap()
    }

    #[cfg(feature = "cluster")]
    fn install_next_assignment(
        scope: &ClusterShuffleConfig,
        owners: [u64; 8],
    ) -> CheckpointAssignmentFence {
        let fence = test_assignment_fence(scope.registry.assignment_version() + 1, &owners);
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
            .set_assignment_and_version(Arc::from(owners.map(NodeId)), fence.assignment_version);
        fence
    }

    #[cfg(feature = "cluster")]
    fn encode_handoff_whole(
        fence: &CheckpointAssignmentFence,
        participant_id: u64,
        frontier: InputFrontier,
        queued: bool,
    ) -> Vec<u8> {
        let channels = fence
            .participant_ids()
            .into_iter()
            .filter(|peer| *peer != participant_id)
            .enumerate()
            .map(|(index, peer)| EowcCheckpointChannel {
                peer,
                applied: frontier.into(),
                events: if queued && index == 0 {
                    vec![EowcCheckpointEvent::Frontier {
                        recovery_gen: 0,
                        frontier: frontier.into(),
                    }]
                } else {
                    Vec::new()
                },
            })
            .collect();
        let checkpoint = EowcOperatorCheckpoint {
            version: OPERATOR_CHECKPOINT_VERSION,
            high_watermark_ms: EowcQueryOperator::frontier_watermark(frontier),
            cluster: Some(EowcClusterCheckpoint {
                assignment_version: fence.assignment_version,
                owner_map_digest: fence.assignment_digest,
                self_id: participant_id,
                local_frontier: frontier.into(),
                effective_frontier: frontier.into(),
                remote_peer_cursor: None,
                channels,
                data_ipc: Vec::new(),
            }),
        };
        rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
            .unwrap()
            .to_vec()
    }

    #[cfg(feature = "cluster")]
    fn projected_batch_for_vnode(
        operator: &EowcQueryOperator,
        vnode: u32,
        price: f64,
    ) -> (String, RecordBatch) {
        let window = operator.state.as_ref().unwrap();
        let projection = window.compiled_projection().unwrap();
        for index in 0..1_000 {
            let symbol = format!("K{index}");
            let raw = RecordBatch::try_new(
                test_schema(),
                vec![
                    Arc::new(StringArray::from(vec![symbol.as_str()])),
                    Arc::new(Float64Array::from(vec![price])),
                    Arc::new(Int64Array::from(vec![100])),
                ],
            )
            .unwrap();
            let projected = projection.evaluate(&raw).unwrap();
            let routed = crate::operator::sql_query::hash_rows_to_vnodes(
                &projected,
                window.num_group_cols(),
                u32::from(key_groups()),
            )
            .unwrap();
            if routed == [vnode] {
                return (symbol, projected);
            }
        }
        panic!("no test key hashes to vnode {vnode}");
    }

    fn materialize_capture(
        capture: crate::operator_graph::CapturedVnodeState,
    ) -> (u32, bytes::Bytes) {
        let state = capture.state.unwrap();
        let mut staged_bytes = state.retained_bytes();
        let bytes = state.materialize(&mut staged_bytes, u64::MAX).unwrap();
        (capture.vnode, bytes)
    }

    fn unaligned_archive_transport(bytes: &[u8]) -> bytes::Bytes {
        let mut transport = vec![0_u8; bytes.len() + CHECKPOINT_ARCHIVE_ALIGNMENT];
        let base = transport.as_ptr() as usize;
        let offset = (0..CHECKPOINT_ARCHIVE_ALIGNMENT)
            .find(|offset| !(base + offset).is_multiple_of(CHECKPOINT_ARCHIVE_ALIGNMENT))
            .expect("an archive transport offset must be unaligned");
        transport[offset..offset + bytes.len()].copy_from_slice(bytes);
        let bytes = bytes::Bytes::from(transport).slice(offset..offset + bytes.len());
        assert_ne!(bytes.as_ptr().align_offset(CHECKPOINT_ARCHIVE_ALIGNMENT), 0);
        bytes
    }

    #[tokio::test]
    async fn grouped_window_restores_exact_unaligned_vnode_frames_and_frontier() {
        let mut original = EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        );
        original.initialize_managed_state().await.unwrap();
        original
            .process(&[vec![test_batch(vec![100, 200])]], &[10_000])
            .await
            .unwrap();

        let required = (0..u32::from(key_groups())).collect::<Vec<_>>();
        let captures = original
            .checkpoint_vnodes(&required, u32::from(key_groups()), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(captures.len(), required.len());
        let frames = captures
            .into_iter()
            .map(materialize_capture)
            .map(|(vnode, state)| (vnode, unaligned_archive_transport(&state)))
            .collect::<Vec<_>>();
        assert!(original
            .checkpoint_vnodes(&required, u32::from(key_groups()), u64::MAX)
            .unwrap()
            .unwrap()
            .is_empty());
        original.process(&[vec![]], &[20_000]).await.unwrap();
        assert!(original
            .checkpoint_vnodes(&required, u32::from(key_groups()), u64::MAX)
            .unwrap()
            .unwrap()
            .is_empty());
        let whole = original.checkpoint().unwrap().unwrap();

        let mut restored = EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        );
        restored.initialize_managed_state().await.unwrap();
        assert!(restored
            .restore_vnode(frames[0].0, u32::from(key_groups()), &frames[0].1)
            .unwrap_err()
            .to_string()
            .contains("whole watermark frame"));
        restored.restore(whole).unwrap();
        #[cfg(feature = "cluster")]
        assert_eq!(restored.restored_output_frontier(), None);
        assert_eq!(restored.state.as_ref().unwrap().high_watermark_ms(), 20_000);
        assert!(restored
            .restore_vnode(1, u32::from(key_groups()), &frames[0].1)
            .is_err());
        for (vnode, state) in &frames {
            restored
                .restore_vnode(*vnode, u32::from(key_groups()), state)
                .unwrap();
        }

        let expected = original.process(&[vec![]], &[60_000]).await.unwrap();
        let actual = restored.process(&[vec![]], &[60_000]).await.unwrap();
        assert_eq!(actual, expected);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_plan_orders_idle_revival_before_data_and_frontier() {
        use laminar_core::shuffle::ShuffleMessage;

        let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
        let mut operator = EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        );
        operator.initialize_managed_state().await.unwrap();
        let (_, projected) = projected_batch_for_vnode(&operator, 1, 42.0);
        operator.attach_cluster_scope(scope.clone());
        let idle = InputFrontier {
            watermark: Some(100),
            idle: true,
        };
        operator.local_frontier = idle;
        operator.last_broadcast = idle;
        operator.effective_frontier = idle;
        let channel = operator.peer_channels.get_mut(&2).unwrap();
        channel.applied = idle;
        channel.accepted = idle;
        let active = InputFrontier {
            watermark: Some(200),
            idle: false,
        };
        let assignment = scope.registry.versioned_snapshot();
        let plan = operator
            .plan_cluster_batches(vec![projected], active, &scope, &assignment, &[2])
            .unwrap();
        assert!(plan.local_batches.is_empty());
        assert_eq!(plan.effective_frontier, active);
        assert_eq!(plan.outbound.len(), 3);
        assert!(matches!(
            &plan.outbound[0],
            (
                2,
                ShuffleMessage::Frontier {
                    watermark: Some(100),
                    idle: false,
                    ..
                }
            )
        ));
        assert!(matches!(
            &plan.outbound[1],
            (
                2,
                ShuffleMessage::Data {
                    routed_vnodes,
                    ..
                }
            ) if routed_vnodes.as_ref() == [1]
        ));
        assert!(matches!(
            &plan.outbound[2],
            (
                2,
                ShuffleMessage::Frontier {
                    watermark: Some(200),
                    idle: false,
                    ..
                }
            )
        ));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn checkpointed_remote_frontiers_compare_in_receiver_domain() {
        let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
        let mut operator = EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        );
        operator.initialize_managed_state().await.unwrap();
        operator.attach_cluster_scope(scope.clone());
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
            .stage_checkpointed_shuffle_frontier(
                "managed_window",
                2,
                active(100),
                assignment,
                recovery,
            )
            .unwrap();
        assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(500));
        operator
            .stage_checkpointed_shuffle_frontier(
                "managed_window",
                2,
                active(150),
                assignment,
                recovery,
            )
            .unwrap();
        assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(500));
        operator
            .stage_checkpointed_shuffle_frontier(
                "managed_window",
                2,
                active(550),
                assignment,
                recovery,
            )
            .unwrap();
        assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(550));
        assert!(operator
            .stage_checkpointed_shuffle_frontier(
                "managed_window",
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
            .stage_checkpointed_shuffle_frontier(
                "managed_window",
                2,
                active(525),
                assignment,
                recovery,
            )
            .is_err());
        assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(550));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn restored_frontier_bootstrap_precedes_live_source_frontier() {
        let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
        let mut operator = EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        );
        operator.initialize_managed_state().await.unwrap();
        let (symbol, local) = projected_batch_for_vnode(&operator, 0, 42.0);
        let buffered = RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(vec![symbol.as_str()])),
                Arc::new(Float64Array::from(vec![42.0])),
                Arc::new(Int64Array::from(vec![1_000])),
            ],
        )
        .unwrap();
        operator.attach_cluster_scope(scope.clone());
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
        operator
            .state
            .as_mut()
            .unwrap()
            .restore_high_watermark_ms(100)
            .unwrap();
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
    async fn zero_admission_send_restarts_once_without_becoming_runnable() {
        let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
        let mut operator = EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        );
        operator.initialize_managed_state().await.unwrap();
        operator.attach_cluster_scope(scope);

        let retry_plan = vec![(
            2,
            ShuffleMessage::Frontier {
                stage: "managed_window".to_string(),
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
        operator.pending_cluster_input = Some(PendingEowcClusterInput {
            local_batches: Vec::new(),
            outbound: None,
            local_frontier: InputFrontier::default(),
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
            .process_cluster(&[Vec::new()], InputFrontier::default())
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
    async fn pending_cluster_send_drains_remote_data_before_committing_local_cut() {
        let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
        let mut operator = EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        );
        operator.initialize_managed_state().await.unwrap();
        let (local_symbol, local) = projected_batch_for_vnode(&operator, 0, 8.0);
        let (remote_symbol, remote) = projected_batch_for_vnode(&operator, 0, 34.0);
        let (_, outbound_batch) = projected_batch_for_vnode(&operator, 1, 1.0);
        assert_eq!(local_symbol, remote_symbol);
        operator.attach_cluster_scope(scope.clone());
        let close = InputFrontier {
            watermark: Some(60_000),
            idle: false,
        };
        let assignment = scope.registry.versioned_snapshot();
        let plan = operator
            .plan_cluster_batches(
                vec![local, outbound_batch],
                close,
                &scope,
                &assignment,
                &[2],
            )
            .unwrap();
        assert_eq!(plan.local_batches.len(), 1);
        assert!(plan
            .outbound
            .iter()
            .any(|(_, message)| matches!(message, ShuffleMessage::Data { .. })));
        let accounted_bytes = operator.cluster_input_plan_bytes(&plan).unwrap();
        let EowcClusterInputPlan {
            local_batches,
            outbound,
            local_frontier,
            effective_frontier: _,
        } = plan;
        let baseline = operator.managed_state_accounting().unwrap().live;
        let (release, wait) = tokio::sync::oneshot::channel();
        let send = tokio::spawn(async move {
            let _ = wait.await;
            drop(outbound);
            (Ok(()), None)
        });
        operator.pending_cluster_input = Some(PendingEowcClusterInput {
            local_batches,
            outbound: None,
            local_frontier,
            send: Some(send),
            accounted_bytes,
        });
        let assignment_version = scope.registry.assignment_version();
        let recovery_gen = scope.receiver.recovery_gen();
        operator
            .stage_checkpointed_shuffle(
                "managed_window",
                crate::operator::RetainedBatch::restored_channel(
                    remote,
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
                "managed_window",
                2,
                close,
                assignment_version,
                recovery_gen,
            )
            .unwrap();
        assert_eq!(operator.queued_remote_events, 2);
        assert_ne!(operator.queued_payload_bytes, 0);
        assert!(operator.deferred_work_is_runnable());

        let output = tokio::time::timeout(
            Duration::from_millis(50),
            operator.process_cluster(&[Vec::new()], InputFrontier::default()),
        )
        .await
        .expect("pending send blocked the graph task")
        .unwrap();
        assert!(output.is_empty());
        assert!(!operator.wants_input());
        assert!(operator.checkpoint_drain_pending());
        assert!(operator.capture_operator_checkpoint(usize::MAX).is_err());
        assert_eq!(operator.local_frontier, InputFrontier::default());
        assert_eq!(operator.queued_remote_events, 1);
        assert_eq!(operator.queued_payload_bytes, 0);
        assert!(!operator
            .pending_cluster_input
            .as_ref()
            .unwrap()
            .send
            .as_ref()
            .unwrap()
            .is_finished());
        assert!(operator.managed_state_accounting().unwrap().live >= baseline + accounted_bytes);

        let output = tokio::time::timeout(
            Duration::from_millis(50),
            operator.process_cluster(&[Vec::new()], InputFrontier::default()),
        )
        .await
        .expect("remote frontier waited for the blocked send")
        .unwrap();
        assert!(output.is_empty());
        assert_eq!(operator.queued_remote_events, 0);
        assert_eq!(operator.peer_channels[&2].applied, close);
        assert_eq!(operator.local_frontier, InputFrontier::default());
        assert!(operator.pending_cluster_input.is_some());
        assert!(!operator.deferred_work_is_runnable());

        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
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
        .expect("pending send task did not finish");
        assert!(!operator.deferred_work_is_runnable());
        let output = operator
            .process_cluster(&[Vec::new()], InputFrontier::default())
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let total = output[0]
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(total.value(0), 42.0);
        assert!(operator.pending_cluster_input.is_none());
        assert_eq!(operator.local_frontier, close);
        assert_eq!(operator.effective_frontier, close);
        assert!(operator.wants_input());
        assert!(!operator.checkpoint_drain_pending());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_channel_checkpoint_replays_data_before_window_close() {
        let scope = cluster_scope([1, 2, 2, 2, 2, 2, 2, 2]).await;
        let mut operator = EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        );
        operator.initialize_managed_state().await.unwrap();
        let (symbol, projected) = projected_batch_for_vnode(&operator, 0, 42.0);
        operator.attach_cluster_scope(scope.clone());
        let idle = InputFrontier {
            watermark: Some(0),
            idle: true,
        };
        let close = InputFrontier {
            watermark: Some(60_000),
            idle: false,
        };
        operator.local_frontier = idle;
        operator.last_broadcast = idle;
        operator.effective_frontier = idle;
        operator
            .state
            .as_mut()
            .unwrap()
            .restore_high_watermark_ms(0)
            .unwrap();
        let channel = operator.peer_channels.get_mut(&2).unwrap();
        channel.applied = idle;
        channel.accepted = idle;
        let assignment_version = scope.registry.assignment_version();
        let recovery_gen = scope.receiver.recovery_gen();
        operator
            .stage_checkpointed_shuffle_frontier(
                "managed_window",
                2,
                InputFrontier {
                    watermark: Some(0),
                    idle: false,
                },
                assignment_version,
                recovery_gen,
            )
            .unwrap();
        let retained = crate::operator::RetainedBatch::restored_channel(
            projected,
            2,
            assignment_version,
            recovery_gen,
            Arc::from([0_u32]),
        );
        operator
            .stage_checkpointed_shuffle("managed_window", retained, i64::MIN)
            .unwrap();
        operator
            .stage_checkpointed_shuffle_frontier(
                "managed_window",
                2,
                close,
                assignment_version,
                recovery_gen,
            )
            .unwrap();
        let checkpoint = operator.checkpoint().unwrap().unwrap();

        let mut restored = EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        );
        restored.initialize_managed_state().await.unwrap();
        restored.attach_cluster_scope(scope);
        restored.restore(checkpoint).unwrap();
        let channel = &restored.peer_channels[&2];
        assert!(matches!(
            &channel.events[0].payload,
            EowcRemoteEventPayload::Frontier(_)
        ));
        assert!(matches!(
            &channel.events[1].payload,
            EowcRemoteEventPayload::Data(_)
        ));
        assert!(matches!(
            &channel.events[2].payload,
            EowcRemoteEventPayload::Frontier(_)
        ));
        assert_eq!(channel.applied, idle);
        assert_eq!(channel.accepted, close);
        assert!(!restored.wants_input());

        let first = restored
            .process_with_frontiers(&[], std::slice::from_ref(&close))
            .await
            .unwrap();
        assert!(first.is_empty());
        let second = restored
            .process_with_frontiers(&[], std::slice::from_ref(&close))
            .await
            .unwrap();
        assert!(second.is_empty());
        let third = restored
            .process_with_frontiers(&[], std::slice::from_ref(&close))
            .await
            .unwrap();
        assert_eq!(third.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let output = &third[0];
        let output_symbol = output
            .column_by_name("symbol")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let total = output
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(output_symbol.value(0), symbol);
        assert_eq!(total.value(0), 42.0);
        assert_eq!(restored.queued_remote_events, 0);
        assert_eq!(restored.effective_frontier, close);
        assert_eq!(restored.state.as_ref().unwrap().high_watermark_ms(), 60_000);
        assert!(restored.wants_input());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn fresh_owner_reconciles_portable_cut_atomically() {
        use crate::operator_graph::{ManagedVnodeRestore, ManagedWholeRestore};

        let new_operator = || {
            EowcQueryOperator::new(
                "managed_window",
                AGG_SQL,
                Some(EmitClause::OnWindowClose),
                Some(test_window_config()),
                aggregate_context(),
                key_groups(),
                None,
            )
        };
        let cut = InputFrontier {
            watermark: Some(20_000),
            idle: false,
        };
        let mut donor = new_operator();
        donor.initialize_managed_state().await.unwrap();
        let (_, projected) = projected_batch_for_vnode(&donor, 0, 42.0);
        let output = EowcQueryOperator::apply_routed_and_close(
            donor.state.as_mut().unwrap(),
            &[(projected, Some(0))],
            10_000,
            "managed_window",
        )
        .unwrap();
        assert!(output.is_empty());
        let frames = donor
            .checkpoint_vnodes(&[0, 1], u32::from(key_groups()), u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(materialize_capture)
            .collect::<Vec<_>>();
        assert_eq!(
            frames.iter().map(|(vnode, _)| *vnode).collect::<Vec<_>>(),
            [0, 1]
        );

        let target_owners = [1, 1, 2, 2, 2, 2, 2, 2];
        let scope = cluster_scope(target_owners).await;
        let _skipped_assignment = install_next_assignment(&scope, target_owners);
        let target_fence = install_next_assignment(&scope, target_owners);
        let predecessor_owners = [2, 3, 2, 2, 2, 2, 2, 2];
        let predecessor =
            test_assignment_fence(target_fence.assignment_version - 2, &predecessor_owners);
        let predecessor_nodes = predecessor_owners.map(NodeId);

        let mut target = new_operator();
        target.initialize_managed_state().await.unwrap();
        target.attach_cluster_scope(scope);
        let pristine_core_bytes = target.state.as_ref().unwrap().accounted_state_bytes();
        let pristine_accounting = target.managed_state_accounting().unwrap();
        let restores = [
            ManagedVnodeRestore {
                participant_id: 2,
                vnode: 0,
                state: frames[0].1.as_ref(),
            },
            ManagedVnodeRestore {
                participant_id: 3,
                vnode: 1,
                state: frames[1].1.as_ref(),
            },
        ];

        let queued_donor = encode_handoff_whole(&predecessor, 2, cut, true);
        let donor3 = encode_handoff_whole(&predecessor, 3, cut, false);
        let queued_whole = [
            ManagedWholeRestore {
                participant_id: 2,
                state: &queued_donor,
            },
            ManagedWholeRestore {
                participant_id: 3,
                state: &donor3,
            },
        ];
        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &queued_whole,
                mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
                    predecessor_owners: &predecessor_nodes,
                },
            })
            .unwrap_err();
        assert_eq!(
            target.managed_state_accounting().unwrap(),
            pristine_accounting
        );
        assert_eq!(target.state.as_ref().unwrap().high_watermark_ms(), i64::MIN);
        assert_eq!(
            target.cluster_assignment.as_ref().unwrap().version(),
            target_fence.assignment_version
        );

        let donor2 = encode_handoff_whole(&predecessor, 2, cut, false);
        let idle_cut = InputFrontier {
            watermark: cut.watermark,
            idle: true,
        };
        let idle_donor = encode_handoff_whole(&predecessor, 3, idle_cut, false);
        let disagreeing_whole = [
            ManagedWholeRestore {
                participant_id: 2,
                state: &donor2,
            },
            ManagedWholeRestore {
                participant_id: 3,
                state: &idle_donor,
            },
        ];
        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &disagreeing_whole,
                mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
                    predecessor_owners: &predecessor_nodes,
                },
            })
            .unwrap_err();
        assert_eq!(
            target.managed_state_accounting().unwrap(),
            pristine_accounting
        );
        assert_eq!(
            target.state.as_ref().unwrap().accounted_state_bytes(),
            pristine_core_bytes
        );
        assert_eq!(target.local_frontier, InputFrontier::default());
        assert_eq!(target.effective_frontier, InputFrontier::default());
        assert_eq!(target.cluster_peers.as_ref(), &[2]);

        let unaligned_donor2 = unaligned_archive_transport(&donor2);
        let unaligned_donor3 = unaligned_archive_transport(&donor3);
        let valid_whole = [
            ManagedWholeRestore {
                participant_id: 2,
                state: &unaligned_donor2,
            },
            ManagedWholeRestore {
                participant_id: 3,
                state: &unaligned_donor3,
            },
        ];
        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &valid_whole,
                mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
                    predecessor_owners: &predecessor_nodes,
                },
            })
            .unwrap();
        assert!(target.managed_state_accounting().unwrap().prepared > 0);
        assert_eq!(target.state.as_ref().unwrap().high_watermark_ms(), i64::MIN);
        assert_eq!(
            target.state.as_ref().unwrap().accounted_state_bytes(),
            pristine_core_bytes
        );
        assert_eq!(target.local_frontier, InputFrontier::default());
        assert_eq!(
            target.cluster_assignment.as_ref().unwrap().version(),
            target_fence.assignment_version
        );

        target.publish_vnode_transition();
        assert_eq!(
            target.cluster_assignment.as_ref().unwrap().version(),
            target_fence.assignment_version
        );
        assert_eq!(
            target.cluster_assignment.as_ref().unwrap().owners(),
            target_owners.map(NodeId)
        );
        assert_eq!(
            target.cluster_assignment_digest,
            Some(target_fence.assignment_digest)
        );
        assert_eq!(target.cluster_peers.as_ref(), &[2]);
        assert_eq!(target.peer_channels.len(), 1);
        let channel = &target.peer_channels[&2];
        assert_eq!(channel.applied, cut);
        assert_eq!(channel.accepted, cut);
        assert!(channel.events.is_empty());
        assert_eq!(target.local_frontier, cut);
        assert_eq!(target.effective_frontier, cut);
        assert_eq!(target.last_broadcast, InputFrontier::default());
        assert!(target.checkpoint_drain_pending());
        assert_eq!(target.remote_peer_cursor, None);
        assert_eq!(target.queued_payload_bytes, 0);
        assert_eq!(target.queued_event_capacity_bytes, 0);
        assert_eq!(target.queued_remote_events, 0);
        assert_eq!(target.state.as_ref().unwrap().high_watermark_ms(), 20_000);
        assert!(target.state.as_ref().unwrap().accounted_state_bytes() > pristine_core_bytes);
        assert!(target.managed_state_accounting().unwrap().retired > 0);
        target.finish_vnode_transition();
        assert_eq!(target.managed_state_accounting().unwrap().retired, 0);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn surviving_owner_preserves_channel_and_bootstraps_new_peer() {
        let predecessor_owners = [1, 2, 1, 4, 1, 5, 1, 5];
        let scope = cluster_scope(predecessor_owners).await;
        let predecessor =
            test_assignment_fence(scope.registry.assignment_version(), &predecessor_owners);
        let mut operator = EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        );
        operator.initialize_managed_state().await.unwrap();
        operator.attach_cluster_scope(scope.clone());
        let effective = InputFrontier {
            watermark: Some(100),
            idle: false,
        };
        let local_before = InputFrontier {
            watermark: Some(80),
            idle: true,
        };
        let local_after = InputFrontier {
            watermark: Some(100),
            idle: true,
        };
        let surviving = InputFrontier {
            watermark: Some(120),
            idle: true,
        };
        let restarted = InputFrontier {
            watermark: Some(140),
            idle: true,
        };
        operator.local_frontier = local_before;
        operator.last_broadcast = local_before;
        operator.effective_frontier = effective;
        operator
            .state
            .as_mut()
            .unwrap()
            .restore_high_watermark_ms(100)
            .unwrap();
        let channel = operator.peer_channels.get_mut(&2).unwrap();
        channel.applied = surviving;
        channel.accepted = surviving;
        let channel = operator.peer_channels.get_mut(&4).unwrap();
        channel.applied = restarted;
        channel.accepted = restarted;
        let channel = operator.peer_channels.get_mut(&5).unwrap();
        channel.applied = effective;
        channel.accepted = effective;

        let target_owners = [1, 2, 1, 4, 1, 3, 1, 3];
        let mut target =
            test_assignment_fence(scope.registry.assignment_version() + 1, &target_owners);
        target
            .participants
            .iter_mut()
            .find(|participant| participant.node_id == 4)
            .unwrap()
            .boot_incarnation = uuid::Uuid::from_u128(44);
        scope
            .sender
            .install_assignment_fence(&target, &target_owners)
            .unwrap();
        scope
            .receiver
            .install_assignment_fence(&target, &target_owners)
            .unwrap();
        scope.registry.set_assignment_and_version(
            Arc::from(target_owners.map(NodeId)),
            target.assignment_version,
        );
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
        assert_eq!(
            operator.cluster_assignment.as_ref().unwrap().version(),
            predecessor.assignment_version
        );
        assert_eq!(operator.cluster_peers.as_ref(), &[2, 4, 5]);
        assert!(!operator.peer_channels.contains_key(&3));
        assert_eq!(operator.peer_channels[&2].applied, surviving);
        assert_eq!(operator.peer_channels[&4].applied, restarted);
        assert_eq!(operator.peer_channels[&5].applied, effective);
        assert_eq!(operator.local_frontier, local_before);
        assert_eq!(operator.last_broadcast, local_before);
        assert_eq!(operator.effective_frontier, effective);
        assert_eq!(operator.state.as_ref().unwrap().high_watermark_ms(), 100);

        operator.publish_vnode_transition();
        assert_eq!(
            operator.cluster_assignment.as_ref().unwrap().version(),
            target.assignment_version
        );
        assert_eq!(
            operator.cluster_assignment.as_ref().unwrap().owners(),
            target_owners.map(NodeId)
        );
        assert_eq!(
            operator.cluster_assignment_digest,
            Some(target.assignment_digest)
        );
        assert_eq!(operator.cluster_peers.as_ref(), &[2, 3, 4]);
        assert_eq!(operator.peer_channels.len(), 3);
        let surviving_channel = &operator.peer_channels[&2];
        assert_eq!(surviving_channel.applied, surviving);
        assert_eq!(surviving_channel.accepted, surviving);
        assert!(surviving_channel.events.is_empty());
        let new_channel = &operator.peer_channels[&3];
        assert_eq!(new_channel.applied, effective);
        assert_eq!(new_channel.accepted, effective);
        assert!(new_channel.events.is_empty());
        let restarted_channel = &operator.peer_channels[&4];
        assert_eq!(restarted_channel.applied, effective);
        assert_eq!(restarted_channel.accepted, effective);
        assert!(restarted_channel.events.is_empty());
        assert!(!operator.peer_channels.contains_key(&5));
        assert_eq!(operator.local_frontier, local_after);
        assert_eq!(operator.effective_frontier, effective);
        assert_eq!(operator.last_broadcast, InputFrontier::default());
        assert!(operator.checkpoint_drain_pending());
        assert_eq!(
            operator
                .normalized_local_frontier(local_before, false)
                .unwrap(),
            local_after
        );
        assert_eq!(
            operator
                .normalized_local_frontier(
                    InputFrontier {
                        watermark: Some(90),
                        idle: false,
                    },
                    false,
                )
                .unwrap(),
            effective
        );
        assert_eq!(operator.remote_peer_cursor, None);
        assert_eq!(operator.queued_payload_bytes, 0);
        assert_eq!(operator.queued_event_capacity_bytes, 0);
        assert_eq!(operator.queued_remote_events, 0);
        assert_eq!(operator.state.as_ref().unwrap().high_watermark_ms(), 100);
        operator.finish_vnode_transition();
        assert_eq!(operator.managed_state_accounting().unwrap().retired, 0);

        operator.last_broadcast = operator.local_frontier;
        let exit_owners = [2, 2, 3, 4, 2, 3, 4, 2];
        let exit = test_assignment_fence(scope.registry.assignment_version() + 1, &exit_owners);
        scope.sender.invalidate_assignment_fence();
        scope.receiver.invalidate_assignment_fence();
        scope.registry.set_assignment_and_version(
            Arc::from(exit_owners.map(NodeId)),
            exit.assignment_version,
        );
        let revoked = [0_u32, 2, 4, 6]
            .into_iter()
            .collect::<rustc_hash::FxHashSet<_>>();
        operator
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &target,
                target: &exit,
                revoked: &revoked,
                restores: &[],
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        operator.publish_vnode_transition();
        assert_eq!(operator.cluster_peers.as_ref(), &[2, 3, 4]);
        assert!(operator.peer_channels.values().all(|channel| {
            channel.applied == effective
                && channel.accepted == effective
                && channel.events.is_empty()
        }));
        assert_eq!(operator.local_frontier, effective);
        assert_eq!(operator.effective_frontier, effective);
        assert_eq!(operator.last_broadcast, effective);
        assert!(!operator.checkpoint_drain_pending());
        operator
            .validate_drained_transition_cut(
                operator.cluster_assignment.as_ref().unwrap(),
                operator.state.as_ref().unwrap(),
                NodeId(1),
            )
            .unwrap();
        operator.finish_vnode_transition();
    }

    #[tokio::test]
    async fn whole_restore_rejects_unwatermarked_live_state() {
        let new_operator = || {
            EowcQueryOperator::new(
                "managed_window",
                AGG_SQL,
                Some(EmitClause::OnWindowClose),
                Some(test_window_config()),
                aggregate_context(),
                key_groups(),
                None,
            )
        };
        let mut donor = new_operator();
        donor.initialize_managed_state().await.unwrap();
        let checkpoint = donor.checkpoint().unwrap().unwrap();

        let mut target = new_operator();
        target.initialize_managed_state().await.unwrap();
        target
            .process(&[vec![test_batch(vec![100])]], &[i64::MIN])
            .await
            .unwrap();
        assert!(!target.state.as_ref().unwrap().is_pristine_for_restore());
        assert!(target.restore(checkpoint).is_err());
    }

    #[test]
    fn test_eowc_operator_creation() {
        let ctx = laminar_sql::create_session_context();
        let op = EowcQueryOperator::new(
            "test_eowc",
            "SELECT symbol, SUM(price) FROM trades GROUP BY symbol",
            Some(EmitClause::OnWindowClose),
            None,
            ctx,
            key_groups(),
            None,
        );
        assert_eq!(&*op.op_name, "test_eowc");
        assert!(op.state.is_none());
    }

    #[test]
    fn core_window_partial_apply_wrapper_preserves_terminal_disposition() {
        let error = EowcQueryOperator::core_window_apply_error(
            "test_eowc",
            "window update",
            DbError::PipelineTerminal("invalid compiled expression".into()),
        );
        assert!(matches!(
            &error,
            DbError::PipelineTerminal(reason) if reason == "invalid compiled expression"
        ));
        assert!(error.requires_pipeline_halt());
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn core_window_shuffle_wrappers_preserve_terminal_disposition() {
        fn assert_terminal(error: DbError, expected: &str) {
            let DbError::ShuffleTerminal(reason) = error else {
                panic!("expected permanent shuffle halt, got {error}");
            };
            assert_eq!(reason, expected);
        }

        let operator = EowcQueryOperator::new(
            "test_eowc",
            "SELECT symbol, SUM(price) FROM trades GROUP BY symbol",
            Some(EmitClause::OnWindowClose),
            None,
            laminar_sql::create_session_context(),
            key_groups(),
            None,
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

    #[test]
    fn test_eowc_checkpoint_uninit_returns_none() {
        let ctx = laminar_sql::create_session_context();
        let mut op = EowcQueryOperator::new(
            "test_eowc",
            "SELECT * FROM trades",
            Some(EmitClause::OnWindowClose),
            None,
            ctx,
            key_groups(),
            None,
        );
        let cp = op.checkpoint().unwrap();
        assert!(cp.is_none());
    }

    #[tokio::test]
    async fn test_eowc_process_empty_inputs() {
        let ctx = aggregate_context();
        let mut op = EowcQueryOperator::new(
            "test_eowc",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            ctx,
            key_groups(),
            None,
        );
        op.initialize_managed_state().await.unwrap();

        let result = op.process(&[vec![]], &[0]).await.unwrap();
        assert!(result.is_empty());
    }
}
