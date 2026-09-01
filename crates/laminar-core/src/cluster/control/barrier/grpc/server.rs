//! Follower RPC admission, authority checks, and prepare waiter ownership.

#[cfg(feature = "cluster")]
use super::{
    ack_disposition_to_wire, assignment_fence_from_wire, barrier_v1, grpc_ack,
    leader_proof_challenge_from_wire, leader_proof_from_wire, validate_wire_checkpoint_attempt,
    Arc, BarrierAck, BarrierAckDisposition, BarrierAnnouncement, BarrierFlavor, BarrierIdentity,
    BarrierProcessIdentity, CheckpointAttempt, PendingPrepareWaiter, Phase, PrepareAckState,
    PrepareWaiterRegistration, MAX_PREPARE_WAITERS_PER_IDENTITY, MAX_RETAINED_BARRIER_IDENTITIES,
    PREPARE_RPC_TIMEOUT,
};

#[cfg(feature = "cluster")]
pub(crate) type LocalLeaderProofProvider =
    Arc<dyn Fn() -> Option<super::super::super::LeaderProof> + Send + Sync>;

#[cfg(feature = "cluster")]
pub(in crate::cluster::control::barrier) struct GrpcBarrierServer {
    pub(in crate::cluster::control::barrier) incoming_tx: crossfire::MAsyncTx<BarrierFlavor>,
    pub(in crate::cluster::control::barrier) prepare_acks: Arc<parking_lot::Mutex<PrepareAckState>>,
    pub(in crate::cluster::control::barrier) leader_lease_store:
        Arc<parking_lot::Mutex<Option<Arc<super::super::super::LeaderLeaseStore>>>>,
    pub(in crate::cluster::control::barrier) local_leader_proof:
        Arc<parking_lot::Mutex<Option<LocalLeaderProofProvider>>>,
    pub(in crate::cluster::control::barrier) local_process:
        Arc<std::sync::OnceLock<BarrierProcessIdentity>>,
    pub(in crate::cluster::control::barrier) process_lease_deadline:
        Arc<std::sync::OnceLock<Arc<super::super::super::LeaseDeadline>>>,
}

#[cfg(feature = "cluster")]
impl GrpcBarrierServer {
    fn require_live_process_lease(
        &self,
    ) -> Result<Option<&super::super::super::LeaseDeadline>, tonic::Status> {
        if self.local_process.get().is_none() {
            return Ok(None);
        }
        let deadline = self.process_lease_deadline.get().ok_or_else(|| {
            tonic::Status::failed_precondition("Process lease deadline is not installed")
        })?;
        if !deadline.is_live() {
            return Err(tonic::Status::failed_precondition(
                "Process lease deadline has expired",
            ));
        }
        Ok(Some(deadline))
    }

    fn require_exact_local_proof_process(
        &self,
        proof: &super::super::super::LeaderProof,
    ) -> Result<(), tonic::Status> {
        let local = self.local_process.get().copied().ok_or_else(|| {
            tonic::Status::failed_precondition(
                "Leader proof confirmation requires a process-bound endpoint",
            )
        })?;
        if local.node_id != proof.owner.node_id
            || local.boot_incarnation != proof.owner.boot_id
            || local.process_term != proof.owner.process_term
        {
            return Err(tonic::Status::failed_precondition(
                "Leader proof challenge does not target this exact process",
            ));
        }
        Ok(())
    }

    async fn enqueue_while_process_live(
        &self,
        announcement: BarrierAnnouncement,
    ) -> Result<(), tonic::Status> {
        let deadline = self.require_live_process_lease()?;
        let send_result = if let Some(deadline) = deadline {
            tokio::select! {
                biased;
                () = deadline.wait_until_expired() => {
                    return Err(tonic::Status::failed_precondition(
                        "Process lease deadline expired before barrier delivery",
                    ));
                }
                result = self.incoming_tx.send(announcement) => result,
            }
        } else {
            self.incoming_tx.send(announcement).await
        };
        if send_result.is_err() {
            return Err(tonic::Status::aborted("Follower coordinator shutdown"));
        }
        self.require_live_process_lease()?;
        Ok(())
    }

    async fn wait_for_prepare_ack(
        &self,
        rx: tokio::sync::oneshot::Receiver<BarrierAck>,
    ) -> Result<BarrierAck, tonic::Status> {
        let deadline = self.require_live_process_lease()?;
        let wait_for_ack = async {
            match tokio::time::timeout(PREPARE_RPC_TIMEOUT, rx).await {
                Ok(Ok(ack)) => Ok(ack),
                Ok(Err(_)) => Err(tonic::Status::internal("Ack sender dropped")),
                Err(_) => Err(tonic::Status::deadline_exceeded(
                    "Follower checkpoint prepare timed out",
                )),
            }
        };
        tokio::pin!(wait_for_ack);
        if let Some(deadline) = deadline {
            tokio::select! {
                biased;
                () = deadline.wait_until_expired() => Err(tonic::Status::failed_precondition(
                    "Process lease deadline expired while awaiting Prepare acknowledgement",
                )),
                result = &mut wait_for_ack => result,
            }
        } else {
            wait_for_ack.await
        }
    }

    fn require_local_assignment_process(
        &self,
        fence: Option<&super::super::super::CheckpointAssignmentFence>,
    ) -> Result<(), tonic::Status> {
        match (self.local_process.get().copied(), fence) {
            (None, None) => Ok(()),
            (Some(_), None) => Err(tonic::Status::failed_precondition(
                "Assignment-less barrier cannot target a process-bound cluster endpoint",
            )),
            (None, Some(_)) => Err(tonic::Status::failed_precondition(
                "Certified barrier cannot target a process-unbound control endpoint",
            )),
            (Some(local), Some(fence))
                if fence.participant_incarnation(local.node_id) == Some(local.boot_incarnation) =>
            {
                Ok(())
            }
            (Some(_), Some(_)) => Err(tonic::Status::failed_precondition(
                "Certified barrier does not target this exact process",
            )),
        }
    }

    pub(in crate::cluster::control::barrier) async fn require_latest_proof(
        &self,
        proof: &super::super::super::LeaderProof,
    ) -> Result<(), tonic::Status> {
        let store = self.leader_lease_store.lock().clone().ok_or_else(|| {
            tonic::Status::failed_precondition("Durable leader lease store is not installed")
        })?;
        let lease = store
            .load()
            .await
            .map_err(|error| {
                tonic::Status::unavailable(format!("Leader lease read failed: {error}"))
            })?
            .ok_or_else(|| tonic::Status::permission_denied("No durable leader lease exists"))?;
        if !lease.matches_proof(proof) {
            return Err(tonic::Status::permission_denied(
                "Leader proof does not match the latest durable lease",
            ));
        }
        Ok(())
    }

    async fn validate_reversible_leader(
        &self,
        proof: Option<barrier_v1::LeaderProof>,
    ) -> Result<super::super::super::LeaderProof, tonic::Status> {
        let proof = leader_proof_from_wire(proof)?
            .ok_or_else(|| tonic::Status::permission_denied("Missing durable leader proof"))?;
        self.require_latest_proof(&proof).await?;
        Ok(proof)
    }
}

#[cfg(feature = "cluster")]
#[tonic::async_trait]
impl barrier_v1::barrier_sync_server::BarrierSync for GrpcBarrierServer {
    async fn confirm_leader_proof(
        &self,
        request: tonic::Request<barrier_v1::LeaderProofChallenge>,
    ) -> Result<tonic::Response<barrier_v1::LeaderProofAck>, tonic::Status> {
        self.require_live_process_lease()?;
        let requested = request.into_inner();
        let expected = leader_proof_from_wire(requested.expected_proof)?
            .ok_or_else(|| tonic::Status::invalid_argument("Leader proof challenge is missing"))?;
        leader_proof_challenge_from_wire(&requested.challenge_id)?;
        self.require_exact_local_proof_process(&expected)?;
        let provider = self.local_leader_proof.lock().clone().ok_or_else(|| {
            tonic::Status::failed_precondition("Local leader proof provider is not installed")
        })?;
        let local = provider().ok_or_else(|| {
            tonic::Status::failed_precondition("No process-local leader grant is live")
        })?;
        if local != expected {
            return Err(tonic::Status::failed_precondition(
                "Live process-local leader grant does not match the durable proof challenge",
            ));
        }
        self.require_live_process_lease()?;
        Ok(tonic::Response::new(barrier_v1::LeaderProofAck {
            challenge_id: requested.challenge_id,
        }))
    }

    async fn prepare(
        &self,
        request: tonic::Request<barrier_v1::PrepareRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        self.require_live_process_lease()?;
        let req = request.into_inner();
        validate_wire_checkpoint_attempt(req.epoch, req.checkpoint_id)?;
        let assignment_fence = assignment_fence_from_wire(
            req.assignment_version,
            req.assignment_vnode_count,
            req.assignment_map_digest,
            req.assignment_participants,
        )?;
        self.require_local_assignment_process(assignment_fence.as_ref())?;
        let leader_proof = self
            .validate_reversible_leader(req.leader_proof.clone())
            .await?;
        let attempt = CheckpointAttempt::new(req.epoch, req.checkpoint_id);
        let assignment_digest = assignment_fence
            .as_ref()
            .map(super::super::super::CheckpointAssignmentFence::digest);
        let identity = BarrierIdentity {
            attempt,
            assignment_digest,
            flags: req.flags,
        };

        self.require_live_process_lease()?;
        let (waiter_id, rx) = {
            let mut state = self.prepare_acks.lock();
            state.record_receipt(identity);
            if let Some(ack) = state.completed.get(&identity) {
                self.require_live_process_lease()?;
                return Ok(tonic::Response::new(grpc_ack(ack.clone())));
            }
            if state.pending.get(&identity).map_or(0, Vec::len) >= MAX_PREPARE_WAITERS_PER_IDENTITY
            {
                return Err(tonic::Status::resource_exhausted(
                    "Too many concurrent retries for one checkpoint Prepare",
                ));
            }
            if !state.pending.contains_key(&identity)
                && state.pending.len() >= MAX_RETAINED_BARRIER_IDENTITIES
            {
                return Err(tonic::Status::resource_exhausted(
                    "Too many concurrent checkpoint Prepare identities",
                ));
            }

            let (tx, rx) = tokio::sync::oneshot::channel::<BarrierAck>();
            let waiter_id = state.next_waiter_id();
            state
                .pending
                .entry(identity)
                .or_default()
                .push(PendingPrepareWaiter {
                    id: waiter_id,
                    response: tx,
                });
            (waiter_id, rx)
        };
        let _registration = PrepareWaiterRegistration {
            state: Arc::clone(&self.prepare_acks),
            identity,
            waiter_id,
        };

        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            assignment_fence,
            leader_proof: Some(leader_proof.clone()),
            phase: Phase::Prepare,
            flags: req.flags,
        };

        self.enqueue_while_process_live(ann).await?;

        let ack = self.wait_for_prepare_ack(rx).await?;
        if ack.epoch != attempt.epoch
            || ack.checkpoint_id != attempt.checkpoint_id
            || ack.assignment_digest != assignment_digest
            || ack.flags != req.flags
        {
            return Err(tonic::Status::failed_precondition(
                "Follower acknowledgement identity mismatch",
            ));
        }
        self.require_latest_proof(&leader_proof).await?;
        self.require_live_process_lease()?;
        Ok(tonic::Response::new(grpc_ack(ack)))
    }

    async fn aligned(
        &self,
        request: tonic::Request<barrier_v1::AlignedRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        self.require_live_process_lease()?;
        let req = request.into_inner();
        validate_wire_checkpoint_attempt(req.epoch, req.checkpoint_id)?;
        let assignment_fence = assignment_fence_from_wire(
            req.assignment_version,
            req.assignment_vnode_count,
            req.assignment_map_digest,
            req.assignment_participants,
        )?;
        self.require_local_assignment_process(assignment_fence.as_ref())?;
        let leader_proof = self
            .validate_reversible_leader(req.leader_proof.clone())
            .await?;

        // Unlike Commit/Abort, Aligned is mid-protocol: the epoch's ack
        // bookkeeping stays untouched — only the announcement is relayed
        // so the pipeline's resume gate can release.
        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            assignment_fence: assignment_fence.clone(),
            leader_proof: Some(leader_proof),
            phase: Phase::Aligned,
            flags: req.flags,
        };
        self.enqueue_while_process_live(ann).await?;
        self.require_live_process_lease()?;
        Ok(tonic::Response::new(barrier_v1::Ack {
            epoch: req.epoch,
            disposition: ack_disposition_to_wire(BarrierAckDisposition::Prepared),
            error: None,
            local_watermark_ms: None,
            checkpoint_id: req.checkpoint_id,
            assignment_digest: assignment_fence
                .as_ref()
                .map_or_else(Vec::new, |fence| fence.digest().to_vec()),
            watermark_status: 0,
            flags: req.flags,
        }))
    }

    async fn commit(
        &self,
        request: tonic::Request<barrier_v1::CommitRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        self.require_live_process_lease()?;
        let req = request.into_inner();
        validate_wire_checkpoint_attempt(req.epoch, req.checkpoint_id)?;
        let leader_proof = leader_proof_from_wire(req.leader_proof.clone())?;
        let assignment_fence = assignment_fence_from_wire(
            req.assignment_version,
            req.assignment_vnode_count,
            req.assignment_map_digest,
            req.assignment_participants,
        )?;
        self.require_local_assignment_process(assignment_fence.as_ref())?;

        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            assignment_fence: assignment_fence.clone(),
            leader_proof,
            phase: Phase::Commit,
            flags: req.flags,
        };
        self.enqueue_while_process_live(ann).await?;
        self.require_live_process_lease()?;
        Ok(tonic::Response::new(barrier_v1::Ack {
            epoch: req.epoch,
            disposition: ack_disposition_to_wire(BarrierAckDisposition::Prepared),
            error: None,
            local_watermark_ms: None,
            checkpoint_id: req.checkpoint_id,
            assignment_digest: assignment_fence
                .as_ref()
                .map_or_else(Vec::new, |fence| fence.digest().to_vec()),
            watermark_status: 0,
            flags: req.flags,
        }))
    }

    async fn abort(
        &self,
        request: tonic::Request<barrier_v1::AbortRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        self.require_live_process_lease()?;
        let req = request.into_inner();
        validate_wire_checkpoint_attempt(req.epoch, req.checkpoint_id)?;
        let leader_proof = leader_proof_from_wire(req.leader_proof.clone())?;
        let assignment_fence = assignment_fence_from_wire(
            req.assignment_version,
            req.assignment_vnode_count,
            req.assignment_map_digest,
            req.assignment_participants,
        )?;
        self.require_local_assignment_process(assignment_fence.as_ref())?;

        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            assignment_fence: assignment_fence.clone(),
            leader_proof,
            phase: Phase::Abort,
            flags: req.flags,
        };
        self.enqueue_while_process_live(ann).await?;
        self.require_live_process_lease()?;
        Ok(tonic::Response::new(barrier_v1::Ack {
            epoch: req.epoch,
            disposition: ack_disposition_to_wire(BarrierAckDisposition::Prepared),
            error: None,
            local_watermark_ms: None,
            checkpoint_id: req.checkpoint_id,
            assignment_digest: assignment_fence
                .as_ref()
                .map_or_else(Vec::new, |fence| fence.digest().to_vec()),
            watermark_status: 0,
            flags: req.flags,
        }))
    }
}
