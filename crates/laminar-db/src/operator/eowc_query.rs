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
#[cfg(feature = "cluster")]
use crate::operator_graph::ManagedVnodeTransition;
use crate::operator_graph::{
    try_evaluate_compiled, EncodedStateFrame, GraphOperator, InputFrontier,
    ManagedStateAccountingSnapshot, OperatorCheckpoint, StateFrameCapture,
};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::WindowOperatorConfig;

#[cfg(feature = "cluster")]
enum CoreWindowTransitionCleanup {
    Aborted(PreparedCoreWindowVnodeTransition),
    Published(RetiredCoreWindowVnodeTransition),
}

const OPERATOR_CHECKPOINT_VERSION: u8 = 2;
const OPERATOR_CHECKPOINT_BASE_SCRATCH: usize = 512;
#[cfg(feature = "cluster")]
const OPERATOR_CAPTURE_ALLOCATION_CHARGE: usize = 32;

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
    outbound: Vec<(u64, laminar_core::shuffle::ShuffleMessage)>,
    local_frontier: InputFrontier,
    effective_frontier: InputFrontier,
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

impl EowcOperatorCheckpointCapture {
    fn encode(self, max_encoded_bytes: usize) -> Result<Vec<u8>, DbError> {
        let mut remaining = max_encoded_bytes
            .checked_sub(OPERATOR_CHECKPOINT_BASE_SCRATCH)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "EOWC operator checkpoint scratch requires {OPERATOR_CHECKPOINT_BASE_SCRATCH} bytes; encoding headroom is {max_encoded_bytes} bytes"
                ))
            })?;
        #[cfg(feature = "cluster")]
        let cluster = if let Some(cluster) = self.cluster {
            let (cluster, next) = cluster.encode(remaining)?;
            remaining = next;
            Some(cluster)
        } else {
            None
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
    prepared_vnode_transition: Option<PreparedCoreWindowVnodeTransition>,
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
    ) -> Result<Vec<RecordBatch>, DbError> {
        let now_filtered = cw.apply_dynamic_now_filter(ctx, inputs, watermark)?;
        let inputs: &[RecordBatch] = now_filtered.as_deref().unwrap_or(inputs);

        let batches = if let Some(proj) = cw.compiled_projection() {
            match try_evaluate_compiled(proj, inputs) {
                Ok(result) => result,
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
            .ok_or_else(|| self.accounting_error())
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
    fn normalized_local_frontier(
        &self,
        input: InputFrontier,
        has_data: bool,
    ) -> Result<InputFrontier, DbError> {
        self.validate_frontier(self.local_frontier, input, "local")?;
        if input.idle && has_data {
            return Err(DbError::InvalidOperation(format!(
                "managed CoreWindow '{}' received data from an idle local channel",
                self.op_name
            )));
        }
        let mut normalized = input;
        if self.local_frontier.idle && !normalized.idle {
            normalized.watermark =
                Self::max_watermark(normalized.watermark, self.effective_frontier.watermark);
        }
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
        let mut remote_data = BTreeMap::<u64, Vec<laminar_core::shuffle::ShuffleMessage>>::new();
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
                    laminar_core::shuffle::ShuffleMessage::checkpointed_routed(
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
                    laminar_core::shuffle::ShuffleMessage::Frontier {
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
                    laminar_core::shuffle::ShuffleMessage::Frontier {
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
    async fn process_cluster(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontier: InputFrontier,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let (config, assignment, peers) = self.active_cluster_scope()?;
        if self.queued_remote_events != 0 {
            if inputs.iter().any(|batches| !batches.is_empty()) {
                return Err(DbError::InvalidOperation(format!(
                    "managed CoreWindow '{}' received local input while ordered shuffle replay was pending",
                    self.op_name
                )));
            }
            return self.drain_remote_event(&assignment, config.self_id);
        }
        let input_batches = inputs.first().map_or(&[][..], Vec::as_slice);
        let has_data = input_batches.iter().any(|batch| batch.num_rows() != 0);
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
            )
            .await?
        };
        let plan =
            self.plan_cluster_batches(pre_aggregate, local_frontier, &config, &assignment, &peers)?;
        let outbound_admitted = !plan.outbound.is_empty();
        if outbound_admitted {
            crate::operator::send_shuffle_plan(
                &config.sender,
                assignment.version(),
                plan.outbound,
                &format!("managed CoreWindow '{}' shuffle", self.op_name),
            )
            .await?;
        }
        let output = {
            let window = self.state.as_mut().expect("initialized CoreWindow state");
            Self::apply_routed_and_close(
                window,
                &plan.local_batches,
                Self::frontier_watermark(plan.effective_frontier),
                &self.op_name,
            )
        }
        .map_err(|error| {
            if !outbound_admitted
                || error.requires_pipeline_recovery()
                || error.requires_pipeline_halt()
            {
                error
            } else {
                DbError::ShufflePartialSend(format!(
                    "managed CoreWindow '{}' failed after outbound shuffle admission: {error}",
                    self.op_name
                ))
            }
        })?;
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
        if self.last_broadcast != self.local_frontier
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
    fn validate_assignment_target(
        &self,
        target: &laminar_core::checkpoint::CheckpointAssignmentFence,
    ) -> Result<(), DbError> {
        let scope = self.cluster_scope.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' has no cluster assignment scope",
                self.op_name
            ))
        })?;
        let assignment = scope.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        if target.vnode_count != scope.registry.vnode_count()
            || target.assignment_version != assignment.version()
            || !target.matches_owner_map(&owners)
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' transition target does not match assignment {}",
                self.op_name,
                assignment.version()
            )));
        }
        Ok(())
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
                .map_or(0, PreparedCoreWindowVnodeTransition::accounted_state_bytes);
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
            )
            .await?
        };
        let routed = pre_aggregate
            .into_iter()
            .map(|batch| (batch, None))
            .collect::<Vec<_>>();
        let window = self.state.as_mut().expect("initialized CoreWindow state");
        Self::apply_routed_and_close(window, &routed, watermark, &self.op_name)
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
        if data.len() > self.max_managed_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "EOWC whole checkpoint for '{}' exceeds its restore limit",
                self.op_name
            )));
        }
        let checkpoint = rkyv::from_bytes::<EowcOperatorCheckpoint, rkyv::rancor::Error>(&data)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "EOWC whole checkpoint deserialization for '{}': {error}",
                    self.op_name
                ))
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
        self.queued_remote_events == 0 && self.last_broadcast == self.local_frontier
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_aligned_replay_pending(&self) -> bool {
        self.queued_remote_events != 0
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_drain_pending(&self) -> bool {
        self.last_broadcast != self.local_frontier
    }

    #[cfg(feature = "cluster")]
    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        if self.cluster_scope.is_none() {
            return input;
        }
        let mut output = self.effective_frontier;
        if self.queued_remote_events != 0 {
            output.idle = false;
        }
        output
    }

    #[cfg(feature = "cluster")]
    fn restored_output_frontier(&self) -> Option<InputFrontier> {
        self.cluster_scope.as_ref()?;
        self.whole_restore_applied
            .then_some(self.effective_frontier)
            .map(|mut frontier| {
                if self.queued_remote_events != 0 {
                    frontier.idle = false;
                }
                frontier
            })
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
        self.validate_frontier(previous, frontier, "accepted remote")?;
        let frontier = if previous.idle && !frontier.idle {
            InputFrontier {
                watermark: Self::max_watermark(
                    frontier.watermark,
                    self.effective_frontier.watermark,
                ),
                idle: false,
            }
        } else {
            frontier
        };
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
        let checkpoint = window.preflight_vnode_bytes(vnode, vnode_count, state)?;
        let checkpoint = rkyv::deserialize::<CoreWindowVnodeCheckpoint, rkyv::rancor::Error>(
            checkpoint.checkpoint,
        )
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "CoreWindow '{}' vnode {vnode} checkpoint deserialization: {error}",
                self.op_name
            ))
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
        self.validate_assignment_target(transition.target)?;
        let Some(window) = self.state.as_ref() else {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow transition targeted uninitialized operator '{}'",
                self.op_name
            )));
        };
        window.validate_vnode_count(transition.target.vnode_count)?;

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
            let state = window.preflight_vnode_bytes(
                restore.vnode,
                transition.target.vnode_count,
                restore.state,
            )?;
            preflighted.push((restore.vnode, state));
        }
        let owned_restores = preflighted.into_iter().map(|(vnode, state)| {
            let state = rkyv::deserialize::<CoreWindowVnodeCheckpoint, rkyv::rancor::Error>(
                state.checkpoint,
            )
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "CoreWindow '{}' vnode {vnode} transition deserialization: {error}",
                    self.op_name
                ))
            })?;
            Ok(crate::core_window_state::OwnedCoreWindowVnodeRestore { vnode, state })
        });
        let prepared = window.prepare_owned_vnode_transition(
            transition.target.vnode_count,
            owned_restores,
            transition.revoked,
        )?;
        self.prepared_vnode_transition = Some(prepared);
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
        let prepared = self
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
        let retired = window.publish_prepared_vnode_transition(prepared);
        self.vnode_transition_cleanup = Some(CoreWindowTransitionCleanup::Published(retired));
    }

    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {
        match self.vnode_transition_cleanup.take() {
            Some(CoreWindowTransitionCleanup::Aborted(prepared)) => drop(prepared),
            Some(CoreWindowTransitionCleanup::Published(retired)) => {
                CoreWindowState::finish_vnode_transition(retired);
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
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
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
        let fence = CheckpointAssignmentFence::from_owner_map(
            registry.assignment_version(),
            &owners,
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

    #[tokio::test]
    async fn grouped_window_restores_exact_vnode_frames_and_frontier() {
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
