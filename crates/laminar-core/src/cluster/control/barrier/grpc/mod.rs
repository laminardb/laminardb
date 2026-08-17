//! Bounded gRPC transport, process-bound endpoints, and phase delivery.

#[cfg(feature = "cluster")]
use super::{
    decode_barrier_endpoint, validate_wire_checkpoint_attempt, watch, Arc, AtomicBool, BarrierAck,
    BarrierAckDisposition, BarrierAnnouncement, BarrierFlavor, BarrierIdentity,
    BarrierProcessIdentity, CheckpointAttempt, CheckpointWatermark, ClusterKv, Duration,
    ExpectedBarrierProcess, FxHashMap, NodeId, NodeInfo, PendingPrepareWaiter, Phase,
    PrepareAckState, PrepareFanoutState, PrepareWaiterRegistration, BARRIER_ADDR_KEY,
    MAX_PREPARE_WAITERS_PER_IDENTITY, MAX_RETAINED_BARRIER_IDENTITIES, PHASE_RPC_TIMEOUT,
    PREPARE_RPC_TIMEOUT,
};

#[cfg(feature = "cluster")]
#[allow(
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::missing_const_for_fn,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    missing_docs
)]
pub(crate) mod barrier_v1 {
    tonic::include_proto!("laminar.barrier.v1");
}

/// Per-peer barrier gRPC client pool. A stable node id may be reused by a replacement process,
/// so every cached channel is bound to the boot incarnation certified by its assignment or
/// leader proof.
#[cfg(feature = "cluster")]
#[derive(Clone)]
pub(super) struct BarrierClientEntry {
    pub(super) process: Option<BarrierProcessIdentity>,
    pub(super) client:
        barrier_v1::barrier_sync_client::BarrierSyncClient<tonic::transport::Channel>,
}

#[cfg(feature = "cluster")]
pub(super) type BarrierClientPool = Arc<parking_lot::Mutex<FxHashMap<NodeId, BarrierClientEntry>>>;

#[cfg(feature = "cluster")]
#[derive(Debug)]
pub(super) enum BarrierClientResolutionError {
    ProcessMismatch,
    Invalid(String),
}

#[cfg(feature = "cluster")]
impl std::fmt::Display for BarrierClientResolutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProcessMismatch => formatter.write_str("endpoint belongs to a different process"),
            Self::Invalid(error) => formatter.write_str(error),
        }
    }
}

#[cfg(feature = "cluster")]
pub(super) fn barrier_client_process_matches(
    expected: Option<ExpectedBarrierProcess>,
    actual: Option<BarrierProcessIdentity>,
) -> bool {
    match (expected, actual) {
        (None, None) => true,
        (Some(expected), Some(actual)) => expected.matches(actual),
        (None, Some(_)) | (Some(_), None) => false,
    }
}

#[cfg(feature = "cluster")]
pub(super) struct GrpcState {
    /// Latest gRPC-delivered announcement, fed in arrival order by the
    /// relay task draining the incoming queue. Latest-wins semantics
    /// (matching the gossip-KV fallback) so concurrent observers — the
    /// pipeline's resume gate and the background durable tail — never
    /// steal announcements from each other.
    pub(super) latest_rx: watch::Receiver<Option<BarrierAnnouncement>>,
    /// Local half of the same ordered announcement stream used by the gRPC server. Cluster
    /// leaders use it to observe their own reversible Aligned notification without polling the
    /// durable fallback.
    pub(super) incoming_tx: crossfire::MAsyncTx<BarrierFlavor>,
    pub(super) merge_error: Arc<parking_lot::Mutex<Option<String>>>,
    pub(super) prepare_acks: Arc<parking_lot::Mutex<PrepareAckState>>,
    /// The one clustered Prepare admitted after durable publication. It progresses from pending
    /// fan-out through claimed quorum collection to an exact quorum-ready Aligned admission.
    pub(super) prepare_fanout: parking_lot::Mutex<Option<PrepareFanoutState>>,
    pub(super) clients: BarrierClientPool,
    pub(super) server_handle: Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub(super) relay_handle: Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub(super) local_process: Arc<std::sync::OnceLock<BarrierProcessIdentity>>,
}

#[cfg(feature = "cluster")]
pub(super) fn abort_grpc_tasks(state: &GrpcState) {
    if let Some(handle) = state.server_handle.lock().take() {
        handle.abort();
    }
    if let Some(handle) = state.relay_handle.lock().take() {
        handle.abort();
    }
}

#[cfg(feature = "cluster")]
pub(super) type ActiveLeaderState =
    Option<(NodeId, watch::Receiver<Vec<NodeInfo>>, Arc<AtomicBool>)>;

#[cfg(feature = "cluster")]
pub(super) fn leader_proof_challenge_from_wire(bytes: &[u8]) -> Result<uuid::Uuid, tonic::Status> {
    let challenge = uuid::Uuid::from_slice(bytes).map_err(|_| {
        tonic::Status::invalid_argument("Leader proof challenge must contain exactly 16 bytes")
    })?;
    if challenge.is_nil() {
        return Err(tonic::Status::invalid_argument(
            "Leader proof challenge must be nonzero",
        ));
    }
    Ok(challenge)
}

#[cfg(feature = "cluster")]
pub(super) fn leader_proof_ack_matches(challenge: uuid::Uuid, acknowledged: &[u8]) -> bool {
    acknowledged == challenge.as_bytes()
}

mod server;

pub(super) use server::GrpcBarrierServer;
pub(crate) use server::LocalLeaderProofProvider;

#[cfg(feature = "cluster")]
#[derive(Clone, Default)]
pub(super) struct WireAssignmentFence {
    pub(super) version: u64,
    pub(super) vnode_count: u32,
    pub(super) map_digest: Vec<u8>,
    pub(super) participants: Vec<barrier_v1::CheckpointParticipant>,
}

#[cfg(feature = "cluster")]
pub(super) fn leader_proof_from_wire(
    proof: Option<barrier_v1::LeaderProof>,
) -> Result<Option<super::super::LeaderProof>, tonic::Status> {
    proof
        .map(|proof| {
            let boot = uuid::Uuid::from_slice(&proof.boot_id).map_err(|_| {
                tonic::Status::invalid_argument(
                    "Leader proof boot identity must contain exactly 16 bytes",
                )
            })?;
            if proof.node_id == 0
                || boot.is_nil()
                || proof.process_term == 0
                || proof.fencing_token == 0
            {
                return Err(tonic::Status::invalid_argument(
                    "Leader proof identity and fencing token must be nonzero",
                ));
            }
            Ok(super::super::LeaderProof {
                owner: crate::checkpoint::LeaderProofOwner {
                    node_id: proof.node_id,
                    boot_id: boot,
                    process_term: proof.process_term,
                },
                fencing_token: proof.fencing_token,
            })
        })
        .transpose()
}

#[cfg(feature = "cluster")]
pub(super) fn leader_proof_to_wire(
    proof: Option<&super::super::LeaderProof>,
) -> Option<barrier_v1::LeaderProof> {
    proof.map(|proof| barrier_v1::LeaderProof {
        node_id: proof.owner.node_id,
        boot_id: proof.owner.boot_id.as_bytes().to_vec(),
        process_term: proof.owner.process_term,
        fencing_token: proof.fencing_token,
    })
}

#[cfg(feature = "cluster")]
pub(super) fn assignment_fence_from_wire(
    version: u64,
    vnode_count: u32,
    map_digest: Vec<u8>,
    participants: Vec<barrier_v1::CheckpointParticipant>,
) -> Result<Option<super::super::CheckpointAssignmentFence>, tonic::Status> {
    if version == 0 && vnode_count == 0 && map_digest.is_empty() && participants.is_empty() {
        return Ok(None);
    }

    let assignment_digest: [u8; 32] = map_digest.try_into().map_err(|_| {
        tonic::Status::invalid_argument(
            "Checkpoint assignment map digest must contain exactly 32 bytes",
        )
    })?;
    let participants = participants
        .into_iter()
        .map(|participant| {
            let boot_incarnation =
                uuid::Uuid::from_slice(&participant.boot_incarnation).map_err(|_| {
                    tonic::Status::invalid_argument(
                        "Checkpoint participant incarnation must contain exactly 16 bytes",
                    )
                })?;
            Ok(super::super::CheckpointParticipant {
                node_id: participant.node_id,
                boot_incarnation,
            })
        })
        .collect::<Result<Vec<_>, tonic::Status>>()?;
    let fence = super::super::CheckpointAssignmentFence {
        assignment_version: version,
        partitioning_abi_version: crate::state::PARTITIONING_ABI_VERSION,
        vnode_count,
        assignment_digest,
        participants,
    };
    if !fence.is_canonical() {
        return Err(tonic::Status::invalid_argument(
            "Non-canonical checkpoint assignment certificate",
        ));
    }
    Ok(Some(fence))
}

#[cfg(feature = "cluster")]
pub(super) fn assignment_fence_to_wire(
    fence: Option<&super::super::CheckpointAssignmentFence>,
) -> WireAssignmentFence {
    fence.map_or_else(WireAssignmentFence::default, |fence| WireAssignmentFence {
        version: fence.assignment_version,
        vnode_count: fence.vnode_count,
        map_digest: fence.assignment_digest.to_vec(),
        participants: fence
            .participants
            .iter()
            .map(|participant| barrier_v1::CheckpointParticipant {
                node_id: participant.node_id,
                boot_incarnation: participant.boot_incarnation.as_bytes().to_vec(),
            })
            .collect(),
    })
}

#[cfg(feature = "cluster")]
pub(super) fn checkpoint_watermark_from_wire(
    status: i32,
    active_watermark_ms: Option<i64>,
) -> Result<CheckpointWatermark, String> {
    use barrier_v1::CheckpointWatermarkStatus as WireStatus;

    let wire_status = WireStatus::try_from(status)
        .map_err(|_| format!("unknown checkpoint watermark status {status}"))?;
    let watermark = match (wire_status, active_watermark_ms) {
        (WireStatus::CheckpointWatermarkUninitialized, None) => CheckpointWatermark::Uninitialized,
        (WireStatus::CheckpointWatermarkIdle, None) => CheckpointWatermark::Idle,
        (WireStatus::CheckpointWatermarkActive, Some(value)) => CheckpointWatermark::Active(value),
        _ => {
            return Err(format!(
                "invalid checkpoint watermark status {status} with active value {active_watermark_ms:?}"
            ));
        }
    };
    watermark.validate()?;
    Ok(watermark)
}

#[cfg(feature = "cluster")]
pub(super) fn ack_disposition_to_wire(disposition: BarrierAckDisposition) -> i32 {
    use barrier_v1::BarrierAckDisposition as WireDisposition;

    match disposition {
        BarrierAckDisposition::Prepared => WireDisposition::BarrierAckPrepared as i32,
        BarrierAckDisposition::PreparedWithReplay => {
            WireDisposition::BarrierAckPreparedWithReplay as i32
        }
        BarrierAckDisposition::Failed => WireDisposition::BarrierAckFailed as i32,
        BarrierAckDisposition::Captured => WireDisposition::BarrierAckCaptured as i32,
        BarrierAckDisposition::CapturedWithReplay => {
            WireDisposition::BarrierAckCapturedWithReplay as i32
        }
    }
}

#[cfg(feature = "cluster")]
pub(super) fn grpc_ack(ack: BarrierAck) -> barrier_v1::Ack {
    use barrier_v1::CheckpointWatermarkStatus as WireStatus;

    let (watermark_status, local_watermark_ms) = match ack.watermark {
        CheckpointWatermark::Uninitialized => {
            (WireStatus::CheckpointWatermarkUninitialized as i32, None)
        }
        CheckpointWatermark::Idle => (WireStatus::CheckpointWatermarkIdle as i32, None),
        CheckpointWatermark::Active(value) => {
            (WireStatus::CheckpointWatermarkActive as i32, Some(value))
        }
    };
    barrier_v1::Ack {
        epoch: ack.epoch,
        disposition: ack_disposition_to_wire(ack.disposition),
        error: ack.error,
        local_watermark_ms,
        checkpoint_id: ack.checkpoint_id,
        assignment_digest: ack
            .assignment_digest
            .map_or_else(Vec::new, |digest| digest.to_vec()),
        watermark_status,
        flags: ack.flags,
    }
}

#[cfg(feature = "cluster")]
pub(super) fn ack_disposition_from_wire(value: i32) -> Result<BarrierAckDisposition, String> {
    use barrier_v1::BarrierAckDisposition as WireDisposition;

    match WireDisposition::try_from(value) {
        Ok(WireDisposition::BarrierAckUnspecified) => {
            Err("barrier acknowledgement disposition is unspecified".into())
        }
        Ok(WireDisposition::BarrierAckPrepared) => Ok(BarrierAckDisposition::Prepared),
        Ok(WireDisposition::BarrierAckPreparedWithReplay) => {
            Ok(BarrierAckDisposition::PreparedWithReplay)
        }
        Ok(WireDisposition::BarrierAckFailed) => Ok(BarrierAckDisposition::Failed),
        Ok(WireDisposition::BarrierAckCaptured) => Ok(BarrierAckDisposition::Captured),
        Ok(WireDisposition::BarrierAckCapturedWithReplay) => {
            Ok(BarrierAckDisposition::CapturedWithReplay)
        }
        Err(_) => Err(format!(
            "unknown barrier acknowledgement disposition {value}"
        )),
    }
}

#[cfg(feature = "cluster")]
pub(super) fn validate_phase_ack(
    ack: &barrier_v1::Ack,
    ann: &BarrierAnnouncement,
) -> Result<(), String> {
    if !CheckpointAttempt::new(ack.epoch, ack.checkpoint_id).is_canonical() {
        return Err("Barrier phase acknowledgement has a non-canonical checkpoint ID".into());
    }
    let expected_digest = ann
        .assignment_fence
        .as_ref()
        .map(super::super::CheckpointAssignmentFence::digest);
    if ack.epoch != ann.epoch
        || ack.checkpoint_id != ann.checkpoint_id
        || ack.flags != ann.flags
        || ack.assignment_digest.as_slice()
            != expected_digest
                .as_ref()
                .map_or(&[][..], <[u8; 32]>::as_slice)
    {
        return Err("Barrier phase acknowledgement identity mismatch".into());
    }
    if ack_disposition_from_wire(ack.disposition)? != BarrierAckDisposition::Prepared {
        return Err(ack
            .error
            .clone()
            .unwrap_or_else(|| "Barrier phase was rejected by follower with no reason".into()));
    }
    Ok(())
}

#[cfg(feature = "cluster")]
pub(super) async fn get_barrier_client(
    peer: NodeId,
    expected_process: Option<ExpectedBarrierProcess>,
    pool: &BarrierClientPool,
    kv: &Arc<dyn ClusterKv>,
) -> Result<
    Option<barrier_v1::barrier_sync_client::BarrierSyncClient<tonic::transport::Channel>>,
    BarrierClientResolutionError,
> {
    {
        let pool = pool.lock();
        if let Some(entry) = pool.get(&peer) {
            if barrier_client_process_matches(expected_process, entry.process) {
                return Ok(Some(entry.client.clone()));
            }
        }
    }

    let Some(raw_endpoint) = kv.read_from(peer, BARRIER_ADDR_KEY).await else {
        return Ok(None);
    };
    let (address, published_process) =
        decode_barrier_endpoint(&raw_endpoint).map_err(BarrierClientResolutionError::Invalid)?;
    if let Some(expected) = expected_process {
        let actual = published_process.ok_or(BarrierClientResolutionError::ProcessMismatch)?;
        if actual.node_id != peer.0 {
            return Err(BarrierClientResolutionError::Invalid(format!(
                "cluster control endpoint slot {} advertises node {}",
                peer.0, actual.node_id
            )));
        }
        if !expected.matches(actual) {
            return Err(BarrierClientResolutionError::ProcessMismatch);
        }
    } else if let Some(process) = published_process {
        if process.node_id != peer.0 {
            return Err(BarrierClientResolutionError::Invalid(format!(
                "cluster control endpoint slot {} advertises a different node",
                peer.0
            )));
        }
        return Err(BarrierClientResolutionError::ProcessMismatch);
    }
    let endpoint = super::super::tls::client_endpoint(&address)
        .map_err(BarrierClientResolutionError::Invalid)?;
    let channel = endpoint.connect_lazy();
    let client = barrier_v1::barrier_sync_client::BarrierSyncClient::new(channel);

    let mut pool = pool.lock();
    if let Some(entry) = pool.get(&peer) {
        if barrier_client_process_matches(expected_process, entry.process) {
            return Ok(Some(entry.client.clone()));
        }
        if expected_process.is_none() && entry.process.is_some() {
            return Ok(Some(client));
        }
    }
    while pool.len() >= crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS && !pool.contains_key(&peer)
    {
        let victim = if expected_process.is_none() {
            pool.iter()
                .find_map(|(node, entry)| entry.process.is_none().then_some(*node))
        } else {
            pool.keys().next().copied()
        };
        let Some(victim) = victim else {
            break;
        };
        pool.remove(&victim);
    }
    if pool.len() >= crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS && !pool.contains_key(&peer) {
        return Ok(Some(client));
    }
    pool.insert(
        peer,
        BarrierClientEntry {
            process: published_process,
            client: client.clone(),
        },
    );
    Ok(Some(client))
}

#[cfg(feature = "cluster")]
pub(super) fn evict_barrier_client(
    pool: &BarrierClientPool,
    peer: NodeId,
    expected_process: Option<ExpectedBarrierProcess>,
) {
    let mut pool = pool.lock();
    let remove = pool
        .get(&peer)
        .is_some_and(|entry| barrier_client_process_matches(expected_process, entry.process));
    if remove {
        pool.remove(&peer);
    }
}

#[cfg(feature = "cluster")]
pub(super) async fn call_phase_rpc(
    client: &mut barrier_v1::barrier_sync_client::BarrierSyncClient<tonic::transport::Channel>,
    ann: &BarrierAnnouncement,
    request_timeout: Duration,
) -> Result<(), String> {
    let assignment = assignment_fence_to_wire(ann.assignment_fence.as_ref());
    match ann.phase {
        Phase::Aligned => {
            let mut request = tonic::Request::new(barrier_v1::AlignedRequest {
                epoch: ann.epoch,
                checkpoint_id: ann.checkpoint_id,
                flags: ann.flags,
                assignment_version: assignment.version,
                assignment_participants: assignment.participants,
                assignment_vnode_count: assignment.vnode_count,
                assignment_map_digest: assignment.map_digest,
                leader_proof: leader_proof_to_wire(ann.leader_proof.as_ref()),
            });
            request.set_timeout(request_timeout);
            client
                .aligned(request)
                .await
                .map_err(|error| error.to_string())
                .and_then(|response| validate_phase_ack(&response.into_inner(), ann))
        }
        Phase::Commit => {
            let mut request = tonic::Request::new(barrier_v1::CommitRequest {
                epoch: ann.epoch,
                checkpoint_id: ann.checkpoint_id,
                flags: ann.flags,
                assignment_version: assignment.version,
                assignment_participants: assignment.participants,
                assignment_vnode_count: assignment.vnode_count,
                assignment_map_digest: assignment.map_digest,
                leader_proof: leader_proof_to_wire(ann.leader_proof.as_ref()),
            });
            request.set_timeout(request_timeout);
            client
                .commit(request)
                .await
                .map_err(|error| error.to_string())
                .and_then(|response| validate_phase_ack(&response.into_inner(), ann))
        }
        Phase::Abort => {
            let mut request = tonic::Request::new(barrier_v1::AbortRequest {
                epoch: ann.epoch,
                checkpoint_id: ann.checkpoint_id,
                flags: ann.flags,
                assignment_version: assignment.version,
                assignment_participants: assignment.participants,
                assignment_vnode_count: assignment.vnode_count,
                assignment_map_digest: assignment.map_digest,
                leader_proof: leader_proof_to_wire(ann.leader_proof.as_ref()),
            });
            request.set_timeout(request_timeout);
            client
                .abort(request)
                .await
                .map_err(|error| error.to_string())
                .and_then(|response| validate_phase_ack(&response.into_inner(), ann))
        }
        Phase::Prepare => Err("Prepare cannot use the phase-notification RPC path".into()),
    }
}

/// Fan a non-Prepare phase announcement to one peer over gRPC. A failed
/// RPC evicts the pooled client so the next round re-resolves the peer.
#[cfg(feature = "cluster")]
pub(super) async fn send_phase_rpc(
    peer: NodeId,
    clients_pool: BarrierClientPool,
    kv: Arc<dyn ClusterKv>,
    ann: BarrierAnnouncement,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    let rpc = match ann.phase {
        Phase::Aligned => "aligned",
        Phase::Commit => "commit",
        Phase::Abort => "abort",
        Phase::Prepare => "prepare",
    };

    let expected_process = ann
        .assignment_fence
        .as_ref()
        .map(|fence| {
            fence
                .participant_incarnation(peer.0)
                .map(|boot| ExpectedBarrierProcess::participant(peer.0, boot))
                .ok_or_else(|| {
                    format!("{rpc} RPC peer {} is outside the assignment roster", peer.0)
                })
        })
        .transpose()?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut client = get_barrier_client(peer, expected_process, &clients_pool, &kv)
            .await
            .map_err(|error| format!("failed to resolve peer {}: {error}", peer.0))?
            .ok_or_else(|| format!("failed to get client for peer {}", peer.0))?;
        let request_timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        call_phase_rpc(&mut client, &ann, request_timeout).await
    })
    .await;

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            evict_barrier_client(&clients_pool, peer, expected_process);
            if tokio::time::Instant::now() >= deadline || error.contains("Timeout expired") {
                Err(format!(
                    "{rpc} RPC to peer {} exceeded its request deadline",
                    peer.0
                ))
            } else {
                Err(format!("{rpc} RPC to peer {} failed: {error}", peer.0))
            }
        }
        Err(_) => {
            evict_barrier_client(&clients_pool, peer, expected_process);
            Err(format!(
                "{rpc} RPC to peer {} exceeded its request deadline",
                peer.0
            ))
        }
    }
}

/// Deliver a non-Prepare phase to remote participants over the low-latency notification path.
#[cfg(feature = "cluster")]
pub(super) async fn send_phase_notifications(
    state: &GrpcState,
    kv: &Arc<dyn ClusterKv>,
    ann: &BarrierAnnouncement,
    expected: Vec<NodeId>,
) -> Vec<Result<(), String>> {
    let deadline = tokio::time::Instant::now() + PHASE_RPC_TIMEOUT;
    futures::future::join_all(expected.into_iter().map(|peer| {
        send_phase_rpc(
            peer,
            Arc::clone(&state.clients),
            Arc::clone(kv),
            ann.clone(),
            deadline,
        )
    }))
    .await
}

#[cfg(feature = "cluster")]
pub(super) fn send_local_phase_notification(
    state: &GrpcState,
    ann: &BarrierAnnouncement,
    process_lease: &super::super::LeaseDeadline,
) -> Result<(), String> {
    if !process_lease.is_live() {
        return Err("local process lease expired before barrier delivery".into());
    }
    match state.incoming_tx.try_send(ann.clone()) {
        Ok(()) => {}
        Err(crossfire::TrySendError::Full(_)) => {
            return Err("local barrier announcement relay is full".into());
        }
        Err(crossfire::TrySendError::Disconnected(_)) => {
            return Err("local barrier announcement relay is closed".into());
        }
    }
    if !process_lease.is_live() {
        return Err("local process lease expired during barrier delivery".into());
    }
    Ok(())
}
