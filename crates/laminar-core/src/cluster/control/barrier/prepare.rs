//! Prepare receipt state, eager fan-out ownership, retry, and acknowledgement validation.

#[cfg(feature = "cluster")]
use super::{
    ack_disposition_from_wire, assignment_fence_to_wire, barrier_v1,
    checkpoint_watermark_from_wire, evict_barrier_client, get_barrier_client, is_terminal_phase,
    leader_proof_to_wire, same_announcement_identity, validate_announcement_attempt, Arc,
    BarrierAck, BarrierAckDisposition, BarrierAnnouncement, BarrierClientPool,
    BarrierClientResolutionError, BarrierProcessIdentity, CheckpointAttempt, CheckpointWatermark,
    ClusterKv, Duration, ExpectedBarrierProcess, FxHashMap, GrpcState, NodeId, Phase,
    WireAssignmentFence, PREPARE_RETRY_INITIAL_BACKOFF, PREPARE_RETRY_MAX_BACKOFF,
};

#[cfg(feature = "cluster")]
pub(super) type BarrierFlavor = crossfire::mpsc::Array<BarrierAnnouncement>;

/// Full retry identity for direct barrier traffic. `CheckpointAttempt` alone is
/// insufficient because an assignment rotation can leave delayed traffic with
/// the same epoch/checkpoint pair but a different participant cut.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct BarrierIdentity {
    pub(super) attempt: CheckpointAttempt,
    pub(super) assignment_digest: Option<[u8; 32]>,
    pub(super) flags: u64,
}

#[cfg(feature = "cluster")]
impl BarrierIdentity {
    pub(super) fn from_announcement(ann: &BarrierAnnouncement) -> Self {
        Self {
            attempt: CheckpointAttempt::new(ann.epoch, ann.checkpoint_id),
            assignment_digest: ann
                .assignment_fence
                .as_ref()
                .map(super::super::CheckpointAssignmentFence::digest),
            flags: ann.flags,
        }
    }

    pub(super) const fn from_ack(ack: &BarrierAck) -> Self {
        Self {
            attempt: CheckpointAttempt::new(ack.epoch, ack.checkpoint_id),
            assignment_digest: ack.assignment_digest,
            flags: ack.flags,
        }
    }
}

#[cfg(feature = "cluster")]
pub(super) const MAX_RETAINED_BARRIER_IDENTITIES: usize = 256;
#[cfg(feature = "cluster")]
pub(super) const MAX_PREPARE_WAITERS_PER_IDENTITY: usize = 32;
#[cfg(feature = "cluster")]
pub(super) const PREPARE_RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Follower-side state shared by the Prepare RPC and the local checkpoint
/// completion path. One exact Prepare may have several retrying RPC waiters;
/// they must all receive the same immutable local result.
#[cfg(feature = "cluster")]
#[derive(Default)]
pub(super) struct PrepareAckState {
    pub(super) pending: FxHashMap<BarrierIdentity, Vec<PendingPrepareWaiter>>,
    pub(super) completed: FxHashMap<BarrierIdentity, BarrierAck>,
    pub(super) received_at: FxHashMap<BarrierIdentity, std::time::Instant>,
    pub(super) next_waiter_id: u64,
}

#[cfg(feature = "cluster")]
pub(super) struct PendingPrepareWaiter {
    pub(super) id: u64,
    pub(super) response: tokio::sync::oneshot::Sender<BarrierAck>,
}

/// Cancellation-safe removal for one Prepare RPC registration. Tonic may drop
/// a handler future when the client deadline expires, so cleanup cannot rely on
/// reaching the explicit timeout branch below.
#[cfg(feature = "cluster")]
pub(super) struct PrepareWaiterRegistration {
    pub(super) state: Arc<parking_lot::Mutex<PrepareAckState>>,
    pub(super) identity: BarrierIdentity,
    pub(super) waiter_id: u64,
}

#[cfg(feature = "cluster")]
impl Drop for PrepareWaiterRegistration {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let remove_entry = state
            .pending
            .get_mut(&self.identity)
            .is_some_and(|waiters| {
                waiters.retain(|waiter| waiter.id != self.waiter_id);
                waiters.is_empty()
            });
        if remove_entry {
            state.pending.remove(&self.identity);
        }
    }
}

#[cfg(feature = "cluster")]
impl PrepareAckState {
    pub(super) fn trim_bounded<K, V>(entries: &mut FxHashMap<K, V>)
    where
        K: Copy + Eq + std::hash::Hash,
    {
        while entries.len() > MAX_RETAINED_BARRIER_IDENTITIES {
            let Some(victim) = entries.keys().next().copied() else {
                break;
            };
            entries.remove(&victim);
        }
    }

    pub(super) fn next_waiter_id(&mut self) -> u64 {
        loop {
            self.next_waiter_id = self.next_waiter_id.wrapping_add(1);
            let candidate = self.next_waiter_id;
            if self
                .pending
                .values()
                .flatten()
                .all(|waiter| waiter.id != candidate)
            {
                return candidate;
            }
        }
    }

    pub(super) fn record_ack(&mut self, identity: BarrierIdentity, ack: &BarrierAck) -> BarrierAck {
        use std::collections::hash_map::Entry;

        let cached = match self.completed.entry(identity) {
            Entry::Vacant(entry) => entry.insert(ack.clone()),
            Entry::Occupied(mut entry) => {
                if ack.disposition.precedence() > entry.get().disposition.precedence() {
                    entry.insert(ack.clone());
                }
                entry.into_mut()
            }
        }
        .clone();
        Self::trim_bounded(&mut self.completed);
        cached
    }

    pub(super) fn record_receipt(&mut self, identity: BarrierIdentity) {
        self.received_at
            .entry(identity)
            .or_insert_with(std::time::Instant::now);
        Self::trim_bounded(&mut self.received_at);
    }
}
/// Typed prepare-failure classification for the quorum wait.
#[cfg(feature = "cluster")]
#[derive(Debug)]
pub(super) enum PeerFailure {
    Unreachable,
    Nack(String),
}

#[cfg(feature = "cluster")]
pub(super) type PrepareTaskResult =
    Result<(NodeId, CheckpointWatermark, bool), (NodeId, PeerFailure)>;

/// Exact announcement and participant roster bound to one eager Prepare round.
#[cfg(feature = "cluster")]
pub(super) struct PrepareFanoutBatch {
    pub(super) announcement: BarrierAnnouncement,
    pub(super) expected: Vec<NodeId>,
    // `JoinSet` aborts all remaining tasks when the batch or quorum future is dropped.
    pub(super) tasks: tokio::task::JoinSet<PrepareTaskResult>,
}

#[cfg(feature = "cluster")]
#[derive(Clone, Copy)]
pub(super) struct PrepareFanoutBudget {
    pub(super) deadline: tokio::time::Instant,
    pub(super) per_attempt: Duration,
}

#[cfg(not(feature = "cluster"))]
pub(super) type PrepareFanoutBudget = ();

#[cfg(feature = "cluster")]
pub(super) fn prepare_fanout_budget(
    attempt_deadline: tokio::time::Instant,
    retry_window: Duration,
) -> Result<PrepareFanoutBudget, String> {
    if attempt_deadline <= tokio::time::Instant::now() {
        return Err("Prepare fan-out attempt deadline must be in the future".into());
    }
    if retry_window.is_zero() {
        return Err("Prepare quorum window must be greater than zero".into());
    }
    let per_attempt = retry_window / 2;
    if per_attempt.is_zero() {
        return Err("Prepare quorum window is too small to divide into retry attempts".into());
    }
    Ok(PrepareFanoutBudget {
        deadline: attempt_deadline,
        per_attempt,
    })
}

#[cfg(feature = "cluster")]
pub(super) enum PrepareFanoutState {
    Pending(PrepareFanoutBatch),
    Claimed(BarrierAnnouncement),
    CaptureQuorumReached(BarrierAnnouncement),
}

#[cfg(feature = "cluster")]
impl PrepareFanoutState {
    pub(super) const fn announcement(&self) -> &BarrierAnnouncement {
        match self {
            Self::Pending(batch) => &batch.announcement,
            Self::Claimed(announcement) | Self::CaptureQuorumReached(announcement) => announcement,
        }
    }
}

#[cfg(feature = "cluster")]
pub(super) fn clustered_prepare_roster(
    prepare: &BarrierAnnouncement,
) -> Result<Option<Vec<NodeId>>, String> {
    if prepare.phase != Phase::Prepare {
        return Err("Prepare fan-out received a different barrier phase".into());
    }
    let Some(fence) = prepare.assignment_fence.as_ref() else {
        // A build with the cluster feature may still run embedded or single-node. Those modes
        // retain the existing KV/direct-on-wait behavior and do not install a clustered batch.
        return Ok(None);
    };
    if !fence.is_canonical() {
        return Err("clustered Prepare has a non-canonical assignment certificate".into());
    }
    let proof = prepare
        .leader_proof
        .as_ref()
        .filter(|proof| proof.is_canonical())
        .ok_or_else(|| "clustered Prepare has no canonical leader proof".to_string())?;
    if fence.participant_incarnation(proof.owner.node_id) != Some(proof.owner.boot_id) {
        return Err(
            "clustered Prepare leader proof is outside its exact assignment process roster".into(),
        );
    }

    Ok(Some(
        fence
            .participants
            .iter()
            .filter(|participant| participant.node_id != proof.owner.node_id)
            .map(|participant| NodeId(participant.node_id))
            .collect(),
    ))
}

#[cfg(feature = "cluster")]
pub(super) fn clustered_phase_roster(
    announcement: &BarrierAnnouncement,
    local_process: Option<BarrierProcessIdentity>,
) -> Result<Option<Vec<NodeId>>, String> {
    if announcement.phase == Phase::Prepare {
        return Err("non-Prepare fan-out received a Prepare announcement".into());
    }
    let Some(fence) = announcement.assignment_fence.as_ref() else {
        return Ok(None);
    };
    if !fence.is_canonical() {
        return Err("clustered barrier has a non-canonical assignment certificate".into());
    }

    let local_process = local_process
        .ok_or_else(|| "assignment-certified phase has no local process identity".to_string())?;
    if announcement.phase == Phase::Aligned {
        let proof = announcement
            .leader_proof
            .as_ref()
            .filter(|proof| proof.is_canonical())
            .ok_or_else(|| "clustered Aligned has no canonical leader proof".to_string())?;
        if fence.participant_incarnation(proof.owner.node_id) != Some(proof.owner.boot_id) {
            return Err(
                "clustered Aligned leader proof is outside its exact assignment process roster"
                    .into(),
            );
        }
        if local_process.node_id != proof.owner.node_id
            || local_process.boot_incarnation != proof.owner.boot_id
            || local_process.process_term != proof.owner.process_term
        {
            return Err("clustered Aligned sender does not own its live leader proof".into());
        }
    }

    Ok(Some(
        fence
            .participants
            .iter()
            .filter(|participant| {
                if is_terminal_phase(announcement.phase) {
                    participant.node_id != local_process.node_id
                } else {
                    !local_process.matches_participant(participant)
                }
            })
            .map(|participant| NodeId(participant.node_id))
            .collect(),
    ))
}

#[cfg(feature = "cluster")]
pub(super) fn prepare_fanout_plan(
    announcement: &BarrierAnnouncement,
    budget: Option<PrepareFanoutBudget>,
) -> Result<(Option<Vec<NodeId>>, Option<PrepareFanoutBudget>), String> {
    if announcement.phase != Phase::Prepare {
        return Ok((None, None));
    }
    let roster = clustered_prepare_roster(announcement)?;
    let budget = roster
        .as_ref()
        .map(|_| {
            budget.ok_or_else(|| {
                "assignment-certified Prepare has no quorum retry window".to_string()
            })
        })
        .transpose()?;
    Ok((roster, budget))
}

#[cfg(feature = "cluster")]
pub(super) fn canonical_expected_roster(expected: &[NodeId]) -> Result<Vec<NodeId>, String> {
    let mut canonical = expected.to_vec();
    canonical.sort_unstable_by_key(|peer| peer.0);
    if canonical.iter().any(NodeId::is_unassigned)
        || canonical.windows(2).any(|pair| pair[0] == pair[1])
    {
        return Err("Prepare quorum participant roster is not canonical".into());
    }
    Ok(canonical)
}

#[cfg(feature = "cluster")]
pub(super) fn install_prepare_fanout(
    state: &GrpcState,
    kv: &Arc<dyn ClusterKv>,
    prepare: &BarrierAnnouncement,
    expected: Vec<NodeId>,
    budget: PrepareFanoutBudget,
) {
    let mut pending = state.prepare_fanout.lock();
    let rpc_deadline = budget.deadline;
    let mut tasks = tokio::task::JoinSet::new();
    for &peer in &expected {
        let clients_pool = Arc::clone(&state.clients);
        let kv = Arc::clone(kv);
        let prepare = prepare.clone();
        tasks.spawn(async move {
            prepare_peer_until_deadline(
                peer,
                clients_pool,
                kv,
                prepare,
                rpc_deadline,
                budget.per_attempt,
            )
            .await
        });
    }
    let incoming = PrepareFanoutBatch {
        announcement: prepare.clone(),
        expected,
        tasks,
    };
    *pending = Some(PrepareFanoutState::Pending(incoming));
}

#[cfg(feature = "cluster")]
pub(super) fn preflight_prepare_fanout(
    state: &GrpcState,
    prepare: &BarrierAnnouncement,
) -> Result<bool, String> {
    let mut pending = state.prepare_fanout.lock();
    let Some(current) = pending.as_ref() else {
        return Ok(true);
    };
    validate_announcement_attempt(prepare)?;
    validate_announcement_attempt(current.announcement())?;
    match prepare
        .checkpoint_id
        .cmp(&current.announcement().checkpoint_id)
    {
        std::cmp::Ordering::Greater => {
            // Admission has already rejected attempt regression. Cancel the obsolete structured
            // fan-out before cancellable durable I/O so it cannot complete after being superseded.
            pending.take();
            Ok(true)
        }
        std::cmp::Ordering::Equal if current.announcement() == prepare => match current {
            PrepareFanoutState::Pending(_) => Ok(false),
            PrepareFanoutState::Claimed(_) => {
                Err("Prepare cannot be republished while its quorum is being collected".into())
            }
            PrepareFanoutState::CaptureQuorumReached(_) => {
                Err("Prepare cannot regress an exact quorum-ready checkpoint".into())
            }
        },
        std::cmp::Ordering::Less => {
            Err("stale Prepare cannot replace a newer in-flight fan-out".into())
        }
        std::cmp::Ordering::Equal => {
            Err("conflicting Prepare certificate cannot replace the in-flight fan-out".into())
        }
    }
}

#[cfg(feature = "cluster")]
pub(super) fn mark_capture_quorum_reached(
    state: &GrpcState,
    prepare: &BarrierAnnouncement,
) -> Result<(), String> {
    let mut fanout = state.prepare_fanout.lock();
    match fanout.take() {
        Some(PrepareFanoutState::Claimed(claimed)) if claimed == *prepare => {
            *fanout = Some(PrepareFanoutState::CaptureQuorumReached(claimed));
            Ok(())
        }
        Some(other) => {
            *fanout = Some(other);
            Err("Prepare quorum completion lost its exact claimed fan-out".into())
        }
        None => Err("Prepare quorum completion has no claimed fan-out".into()),
    }
}

#[cfg(feature = "cluster")]
pub(super) fn require_aligned_quorum(
    state: &GrpcState,
    aligned: &BarrierAnnouncement,
) -> Result<(), String> {
    let fanout = state.prepare_fanout.lock();
    let Some(PrepareFanoutState::CaptureQuorumReached(prepare)) = fanout.as_ref() else {
        return Err("clustered Aligned requires a successful exact capture quorum".into());
    };
    if !same_announcement_identity(prepare, aligned) {
        return Err("clustered Aligned does not match the exact reached Prepare quorum".into());
    }
    Ok(())
}

#[cfg(feature = "cluster")]
pub(super) fn retire_prepare_fanout(state: &GrpcState) {
    state.prepare_fanout.lock().take();
}

#[cfg(feature = "cluster")]
pub(super) fn retryable_prepare_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        // Tonic maps a client-service readiness failure to `Unknown`; it is
        // transport state, not a response from the follower. The remaining
        // codes represent a connection that cannot currently complete the
        // request. Fence and validation failures use distinct semantic codes.
        tonic::Code::Unknown
            | tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Aborted
    )
}

#[cfg(feature = "cluster")]
pub(super) async fn wait_for_prepare_retry(
    deadline: tokio::time::Instant,
    backoff: &mut Duration,
) -> bool {
    let now = tokio::time::Instant::now();
    if now >= deadline {
        return false;
    }

    tokio::time::sleep_until((now + *backoff).min(deadline)).await;
    *backoff = backoff.saturating_mul(2).min(PREPARE_RETRY_MAX_BACKOFF);
    tokio::time::Instant::now() < deadline
}

#[cfg(feature = "cluster")]
pub(super) fn prepare_rpc_request(
    prepare: &BarrierAnnouncement,
    assignment: &WireAssignmentFence,
    leader_proof: Option<&barrier_v1::LeaderProof>,
    timeout: Duration,
) -> tonic::Request<barrier_v1::PrepareRequest> {
    let mut request = tonic::Request::new(barrier_v1::PrepareRequest {
        epoch: prepare.epoch,
        checkpoint_id: prepare.checkpoint_id,
        flags: prepare.flags,
        assignment_version: assignment.version,
        assignment_participants: assignment.participants.clone(),
        assignment_vnode_count: assignment.vnode_count,
        assignment_map_digest: assignment.map_digest.clone(),
        leader_proof: leader_proof.cloned(),
    });
    request.set_timeout(timeout);
    request
}

#[cfg(feature = "cluster")]
pub(super) fn validate_capture_ack(
    peer: NodeId,
    prepare: &BarrierAnnouncement,
    assignment_digest: Option<&[u8; 32]>,
    ack: &barrier_v1::Ack,
) -> Result<(NodeId, CheckpointWatermark, bool), (NodeId, PeerFailure)> {
    if ack.epoch != prepare.epoch
        || ack.checkpoint_id != prepare.checkpoint_id
        || ack.flags != prepare.flags
        || ack.assignment_digest.as_slice()
            != assignment_digest.map_or(&[][..], <[u8; 32]>::as_slice)
    {
        return Err((
            peer,
            PeerFailure::Nack("Prepare acknowledgement identity mismatch".into()),
        ));
    }
    let reason = || {
        ack.error
            .clone()
            .unwrap_or_else(|| "Prepare acknowledgement has no reason".to_string())
    };
    let handoff_replay_pending = match ack_disposition_from_wire(ack.disposition) {
        Ok(BarrierAckDisposition::Captured) => false,
        Ok(BarrierAckDisposition::CapturedWithReplay) => {
            if prepare.flags & crate::checkpoint::flags::HANDOFF == 0 {
                return Err((
                    peer,
                    PeerFailure::Nack(
                        "Prepare acknowledgement retained replay without the HANDOFF flag".into(),
                    ),
                ));
            }
            true
        }
        Ok(BarrierAckDisposition::Prepared | BarrierAckDisposition::PreparedWithReplay) => {
            return Err((
                peer,
                PeerFailure::Nack(
                    "Prepare requires an explicit Captured acknowledgement from every participant"
                        .into(),
                ),
            ));
        }
        Ok(BarrierAckDisposition::Failed) => {
            return Err((peer, PeerFailure::Nack(reason())));
        }
        Err(error) => return Err((peer, PeerFailure::Nack(error))),
    };
    checkpoint_watermark_from_wire(ack.watermark_status, ack.local_watermark_ms)
        .map(|watermark| (peer, watermark, handoff_replay_pending))
        .map_err(|error| (peer, PeerFailure::Nack(error)))
}

#[cfg(feature = "cluster")]
pub(super) fn prepare_expected_process(
    peer: NodeId,
    prepare: &BarrierAnnouncement,
) -> Result<Option<ExpectedBarrierProcess>, (NodeId, PeerFailure)> {
    let Some(fence) = prepare.assignment_fence.as_ref() else {
        return Ok(None);
    };
    let boot = fence.participant_incarnation(peer.0).ok_or_else(|| {
        (
            peer,
            PeerFailure::Nack("Prepare peer is outside the assignment process roster".into()),
        )
    })?;
    Ok(Some(ExpectedBarrierProcess::participant(peer.0, boot)))
}

#[cfg(feature = "cluster")]
pub(super) async fn prepare_peer_until_deadline(
    peer: NodeId,
    clients_pool: BarrierClientPool,
    kv: Arc<dyn ClusterKv>,
    prepare: BarrierAnnouncement,
    deadline: tokio::time::Instant,
    max_attempt_duration: Duration,
) -> Result<(NodeId, CheckpointWatermark, bool), (NodeId, PeerFailure)> {
    let assignment = assignment_fence_to_wire(prepare.assignment_fence.as_ref());
    let assignment_digest = prepare
        .assignment_fence
        .as_ref()
        .map(super::super::CheckpointAssignmentFence::digest);
    let leader_proof = leader_proof_to_wire(prepare.leader_proof.as_ref());
    let expected_process = prepare_expected_process(peer, &prepare)?;
    let mut backoff = PREPARE_RETRY_INITIAL_BACKOFF;

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err((peer, PeerFailure::Unreachable));
        }

        let Ok(client) = tokio::time::timeout_at(
            deadline,
            get_barrier_client(peer, expected_process, &clients_pool, &kv),
        )
        .await
        else {
            return Err((peer, PeerFailure::Unreachable));
        };
        let client = match client {
            Ok(client) => client,
            Err(BarrierClientResolutionError::ProcessMismatch) => {
                if wait_for_prepare_retry(deadline, &mut backoff).await {
                    continue;
                }
                return Err((peer, PeerFailure::Unreachable));
            }
            Err(BarrierClientResolutionError::Invalid(error)) => {
                return Err((peer, PeerFailure::Nack(error)));
            }
        };
        let Some(mut client) = client else {
            if wait_for_prepare_retry(deadline, &mut backoff).await {
                continue;
            }
            return Err((peer, PeerFailure::Unreachable));
        };

        let now = tokio::time::Instant::now();
        let remaining = deadline.saturating_duration_since(now);
        if remaining.is_zero() {
            evict_barrier_client(&clients_pool, peer, expected_process);
            return Err((peer, PeerFailure::Unreachable));
        }
        // Leave part of the existing quorum budget available to evict and re-resolve a lazy
        // channel whose readiness check stalls. Prepare identities are idempotent, so a live
        // follower may safely complete an earlier attempt and serve its cached acknowledgement.
        let attempt_budget = (remaining / 2).min(max_attempt_duration);
        if attempt_budget.is_zero() {
            evict_barrier_client(&clients_pool, peer, expected_process);
            return Err((peer, PeerFailure::Unreachable));
        }
        let attempt_deadline = now + attempt_budget;

        let request =
            prepare_rpc_request(&prepare, &assignment, leader_proof.as_ref(), attempt_budget);

        match tokio::time::timeout_at(attempt_deadline, client.prepare(request)).await {
            Ok(Ok(response)) => {
                return validate_capture_ack(
                    peer,
                    &prepare,
                    assignment_digest.as_ref(),
                    &response.into_inner(),
                );
            }
            Ok(Err(status)) => {
                evict_barrier_client(&clients_pool, peer, expected_process);
                if retryable_prepare_status(&status) {
                    if wait_for_prepare_retry(deadline, &mut backoff).await {
                        continue;
                    }
                    return Err((peer, PeerFailure::Unreachable));
                }
                return Err((peer, PeerFailure::Nack(status.to_string())));
            }
            Err(_) => {
                evict_barrier_client(&clients_pool, peer, expected_process);
                if wait_for_prepare_retry(deadline, &mut backoff).await {
                    continue;
                }
                return Err((peer, PeerFailure::Unreachable));
            }
        }
    }
}
