//! Leader and process-lease authority, fencing, and durable assignment decisions.

use super::*;

impl ClusterController {
    /// This instance's ID.
    #[must_use]
    pub fn instance_id(&self) -> NodeId {
        self.instance_id
    }

    /// The cluster gossip KV, for advertising/discovering per-stream state.
    #[must_use]
    pub fn kv(&self) -> &Arc<dyn ClusterKv> {
        &self.kv
    }

    /// Current leader: the lowest available active certified participant. A locally certified
    /// incumbent may retain candidacy while it drains, but a remote draining process is never
    /// nominated. When no certified participant is available, only a controller wired to durable
    /// leader fencing may nominate the lowest idle worker so it can repair assignment authority.
    #[must_use]
    pub fn current_leader(&self) -> Option<NodeId> {
        let members = self.members_rx.borrow();
        let participants = self.leadership_participants.read();
        let unresponsive = self.unresponsive.lock();
        let is_participant = |node: NodeId| {
            participants
                .as_ref()
                .is_none_or(|(_, roster)| roster.binary_search(&node.0).is_ok())
        };
        let mut eligible: Vec<NodeId> = members
            .iter()
            .filter(|m| {
                m.id != self.instance_id
                    && matches!(m.state, NodeState::Active)
                    && !unresponsive.contains_key(&m.id.0)
            })
            .map(|m| m.id)
            .collect();
        if self.leader_eligible.load(Ordering::SeqCst)
            && self.process_lease_is_live()
            && !unresponsive.contains_key(&self.instance_id.0)
        {
            eligible.push(self.instance_id);
        }
        let certified = eligible
            .iter()
            .copied()
            .filter(|node| is_participant(*node))
            .collect::<Vec<_>>();
        if let Some(leader) = leader_of(&certified) {
            return Some(leader);
        }
        #[cfg(feature = "cluster")]
        if self.has_leader_lease_fencing() {
            return leader_of(&eligible);
        }
        None
    }

    /// True if this node is the gossip-elected candidate (lowest active id), ignoring the
    /// lease. The lease manager acquires only while this holds.
    #[must_use]
    pub fn is_gossip_leader(&self) -> bool {
        self.current_leader() == Some(self.instance_id)
    }

    /// True if this node may act as leader — the gate all leader-gated work inherits. With a
    /// lease wired, also requires this exact process's live local-monotonic grant.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        if !self.process_lease_is_live() {
            return false;
        }
        if !self.is_gossip_leader() {
            return false;
        }
        #[cfg(feature = "cluster")]
        if let Some(gate) = self.leader_lease.get() {
            let lease = gate.lease.borrow();
            let deadline = gate.deadline.borrow().clone();
            return super::super::lease_grants_leadership(&lease, &gate.owner, &deadline);
        }
        true
    }

    /// Whether this controller is wired to a durable leader lease. Gossip-only leadership is
    /// adequate for best-effort coordination but cannot certify exactly-once cluster decisions.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn has_leader_lease_fencing(&self) -> bool {
        self.leader_lease.get().is_some()
    }

    /// Current live fencing token when this node owns the durable lease.
    ///
    /// This is an observation for detecting that an operation crossed leadership terms. Reading
    /// it does **not** fence a later object-store write: a durable mutation is fenced only when
    /// the storage operation atomically validates the token. Callers must not stamp this value
    /// into an otherwise unconditional write and treat that as a correctness boundary.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn leader_fencing_token(&self) -> Option<u64> {
        self.capture_leader_proof().map(|proof| proof.fencing_token)
    }

    /// Capture the exact durable leader term currently authorized by local monotonic leases.
    ///
    /// Gossip determines candidacy only. A proof is available solely when this process is the
    /// candidate, both process and leader deadlines are live, and the observed durable grant has
    /// this process's exact owner identity.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn capture_leader_proof(&self) -> Option<super::super::LeaderProof> {
        if !self.process_lease_proof_is_live() || !self.is_gossip_leader() {
            return None;
        }
        let gate = self.leader_lease.get()?;
        let lease = gate.lease.borrow();
        let deadline = gate.deadline.borrow().clone();
        if !super::super::lease_grants_leadership(&lease, &gate.owner, &deadline) {
            return None;
        }
        lease.as_ref().map(super::super::LeaderLease::proof)
    }

    /// Whether a captured proof remains the exact current locally authorized leader term.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn proof_is_live(&self, proof: &super::super::LeaderProof) -> bool {
        if !self.process_lease_proof_is_live() || !self.is_gossip_leader() {
            return false;
        }
        let Some(gate) = self.leader_lease.get() else {
            return false;
        };
        let lease = gate.lease.borrow();
        let deadline = gate.deadline.borrow().clone();
        super::super::lease_grants_proof(&lease, &gate.owner, &deadline, proof)
    }

    /// Capture the durable leader grant while forming a cluster with no active member.
    ///
    /// Every joining process may contend in a cold start, but the shared lease store admits only
    /// one exact owner. As soon as an active member is visible, a joining process ceases to be a
    /// candidate and cannot use its observation to publish catalog state.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn capture_catalog_bootstrap_proof(&self) -> Option<super::super::LeaderProof> {
        if !self.process_lease_proof_is_live() || !self.is_leader_lease_candidate() {
            return None;
        }
        let gate = self.leader_lease.get()?;
        let lease = gate.lease.borrow();
        let deadline = gate.deadline.borrow().clone();
        if !super::super::lease_grants_leadership(&lease, &gate.owner, &deadline) {
            return None;
        }
        lease.as_ref().map(super::super::LeaderLease::proof)
    }

    /// Whether a catalog-bootstrap proof remains owned by this cold-start candidate.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn catalog_bootstrap_proof_is_live(&self, proof: &super::super::LeaderProof) -> bool {
        if !self.process_lease_proof_is_live() || !self.is_leader_lease_candidate() {
            return false;
        }
        let Some(gate) = self.leader_lease.get() else {
            return false;
        };
        let lease = gate.lease.borrow();
        let deadline = gate.deadline.borrow().clone();
        super::super::lease_grants_proof(&lease, &gate.owner, &deadline, proof)
    }

    /// Subscribe to leader-grant changes for evented proof cancellation.
    ///
    /// After notification, callers must use [`Self::proof_is_live`]. Candidacy changes are
    /// available separately through [`Self::leader_candidacy_watch`]. The lease manager publishes
    /// `None` when its monotonic deadline expires, so no polling loop is required.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn leader_grant_watch(&self) -> Option<watch::Receiver<Option<super::super::LeaderLease>>> {
        self.leader_lease.get().map(|gate| gate.lease.clone())
    }

    #[cfg(feature = "cluster")]
    pub(super) fn process_lease_proof_is_live(&self) -> bool {
        self.process_lease_live.load(Ordering::Acquire)
            && self
                .process_lease_deadline
                .get()
                .is_some_and(|deadline| deadline.is_live())
    }

    /// Wire the exact owner and one fixed local deadline used to fence leader work.
    ///
    /// This compatibility seam is intended for fixed-generation tests and externally managed
    /// grants. A running [`super::super::LeaderLeaseManager`] must use
    /// [`Self::set_leader_lease_runtime_watches`] so deadline rotation remains visible.
    ///
    /// # Errors
    /// Rejects an owner for another node or duplicate installation.
    #[cfg(feature = "cluster")]
    pub fn set_leader_lease_watch(
        &self,
        lease: watch::Receiver<Option<super::super::LeaderLease>>,
        owner: super::super::LeaderLeaseOwner,
        deadline: Arc<super::super::LeaseDeadline>,
    ) -> Result<(), String> {
        let (_deadline_tx, deadline) = watch::channel(deadline);
        self.set_leader_lease_runtime_watches(lease, owner, deadline)
    }

    /// Wire the exact owner and generation-scoped local deadlines used to fence leader work.
    ///
    /// Readers hold the lease observation while sampling its deadline generation. The manager
    /// fences the old deadline, withdraws the old lease, then publishes the next deadline, so a
    /// stale lease can never be paired with a later live generation.
    ///
    /// # Errors
    /// Rejects an owner for another node or duplicate installation.
    #[cfg(feature = "cluster")]
    pub fn set_leader_lease_runtime_watches(
        &self,
        lease: watch::Receiver<Option<super::super::LeaderLease>>,
        owner: super::super::LeaderLeaseOwner,
        deadline: watch::Receiver<Arc<super::super::LeaseDeadline>>,
    ) -> Result<(), String> {
        if owner.node != self.instance_id || owner.boot.is_nil() || owner.process_term == 0 {
            return Err("leader lease owner does not match this process".into());
        }
        self.leader_lease
            .set(LeaderLeaseGate {
                lease,
                owner,
                deadline,
            })
            .map_err(|_| "leader lease fencing is already installed".into())
    }

    pub(super) fn notify_leader_eligibility_change(&self) {
        let eligible = self.is_leader_lease_candidate();
        self.leader_candidacy.send_if_modified(|published| {
            let next = published
                .transition(eligible)
                .unwrap_or_else(super::super::LeaderCandidacy::terminal);
            if next == *published {
                false
            } else {
                *published = next;
                true
            }
        });
    }

    pub(super) fn is_leader_lease_candidate(&self) -> bool {
        if self.is_gossip_leader() {
            return true;
        }
        !self.active.load(Ordering::Acquire)
            && !self.is_draining()
            && self.process_lease_is_live()
            && !self
                .members_rx
                .borrow()
                .iter()
                .any(|member| matches!(member.state, NodeState::Active))
    }

    /// Evented lease candidacy, including cold cluster formation and normal gossip leadership.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn leader_candidacy_watch(
        self: &Arc<Self>,
    ) -> watch::Receiver<super::super::LeaderCandidacy> {
        let mut members = self.members_rx.clone();
        members.borrow_and_update();
        self.notify_leader_eligibility_change();
        let candidacy = self.leader_candidacy.clone();
        let candidate_rx = candidacy.subscribe();
        let controller = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = candidacy.closed() => return,
                    changed = members.changed() => {
                        if changed.is_err() {
                            candidacy.send_replace(super::super::LeaderCandidacy::terminal());
                            return;
                        }
                    }
                }
                let Some(controller) = controller.upgrade() else {
                    return;
                };
                controller.notify_leader_eligibility_change();
            }
        });
        candidate_rx
    }

    /// Mark this node's active status.
    pub fn set_active(&self, active: bool) {
        let _transition = self.process_authority_transition.lock();
        let active = active && self.process_lease_is_live();
        self.active.store(active, Ordering::SeqCst);
        let eligible = active && !self.is_draining();
        if self.leader_eligible.swap(eligible, Ordering::SeqCst) != eligible {
            self.notify_leader_eligibility_change();
        }
    }

    /// Permanently fence controller mutations after stable-node lease loss.
    pub fn fence_process_lease(&self) {
        let _transition = self.process_authority_transition.lock();
        self.process_lease_live.store(false, Ordering::SeqCst);
        if let Some(deadline) = self.process_lease_deadline.get() {
            deadline.fence();
        }
        self.active.store(false, Ordering::SeqCst);
        self.recovering.store(true, Ordering::SeqCst);
        let roster_changed = self.leadership_participants.write().take().is_some();
        if self.leader_eligible.swap(false, Ordering::SeqCst) || roster_changed {
            self.notify_leader_eligibility_change();
        }
        self.checkpoint_assignment_fence.send_replace(None);
        self.checkpoint_drain_transition.send_replace(None);
    }

    /// Whether this process still owns its stable node identity.
    #[must_use]
    pub fn process_lease_is_live(&self) -> bool {
        self.process_lease_live.load(Ordering::Acquire)
            && self
                .process_lease_deadline
                .get()
                .is_none_or(|deadline| deadline.is_live())
    }

    pub(super) fn recovery_process_lease_is_live(&self) -> bool {
        self.process_lease_live.load(Ordering::Acquire)
            && self
                .process_lease_deadline
                .get()
                .is_some_and(|deadline| deadline.is_live())
    }

    /// Wait until the process-local lease deadline expires or is explicitly fenced.
    pub async fn wait_for_process_lease_loss(&self) {
        if !self.process_lease_live.load(Ordering::Acquire) {
            return;
        }
        let Some(deadline) = self.process_lease_deadline.get() else {
            std::future::pending::<()>().await;
            return;
        };
        deadline.wait_until_expired().await;
    }

    /// Install the stable-node lease deadline before the runtime starts.
    ///
    /// Reinstalling the same shared deadline is idempotent. A different deadline, or installation
    /// after terminal process fencing, is rejected so controller and data-plane gates cannot
    /// silently follow different lease clocks.
    ///
    /// # Errors
    /// Rejects installation after fencing or replacement with a different deadline.
    pub fn set_process_lease_deadline(
        &self,
        deadline: Arc<super::super::LeaseDeadline>,
    ) -> Result<(), String> {
        let _transition = self.process_authority_transition.lock();
        if !self.process_lease_live.load(Ordering::Acquire) {
            return Err("process lease authority is already terminally fenced".into());
        }
        if let Some(current) = self.process_lease_deadline.get() {
            if !Arc::ptr_eq(current, &deadline) {
                return Err("process lease deadline is already installed".into());
            }
            #[cfg(feature = "cluster")]
            self.barrier.install_process_lease_deadline(deadline)?;
            return Ok(());
        }
        #[cfg(feature = "cluster")]
        self.barrier
            .install_process_lease_deadline(Arc::clone(&deadline))?;
        self.process_lease_deadline
            .set(deadline)
            .map_err(|_| "process lease deadline is already installed".to_string())
    }

    /// Shared process-local lease deadline, when cluster runtime wiring is complete.
    #[must_use]
    pub fn process_lease_deadline(&self) -> Option<Arc<super::super::LeaseDeadline>> {
        self.process_lease_deadline.get().cloned()
    }

    /// Install the shared stable-node fencing authority before recovery starts.
    ///
    /// # Errors
    /// Rejects replacing an already-installed authority.
    pub fn set_process_lease_authority(
        &self,
        authority: Arc<super::super::process_lease::ProcessLeaseAuthority>,
    ) -> Result<(), String> {
        self.process_lease_authority
            .set(authority)
            .map_err(|_| "process lease authority is already installed".to_string())
    }

    /// Prove that an exact assignment participant was durably superseded within `deadline`.
    ///
    /// # Errors
    /// Fails closed when no shared authority is installed or fencing cannot be proven in time.
    pub async fn fence_process_incarnation(
        &self,
        participant: CheckpointParticipant,
        deadline: tokio::time::Instant,
    ) -> Result<super::super::process_lease::ProcessLeaseFence, String> {
        self.process_lease_authority
            .get()
            .ok_or_else(|| "process lease authority is not installed".to_string())?
            .fence_incarnation(participant, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    /// Bound process fencing by the authority TTL plus the caller's existing control-plane I/O
    /// budget, avoiding a second configurable timeout dimension.
    ///
    /// # Errors
    /// Fails when no shared authority is installed or the deadline cannot be represented.
    pub fn process_fencing_deadline(
        &self,
        io_budget: Duration,
    ) -> Result<tokio::time::Instant, String> {
        self.process_lease_authority
            .get()
            .ok_or_else(|| "process lease authority is not installed".to_string())?
            .fencing_deadline(io_budget)
            .map_err(|error| error.to_string())
    }

    /// Verify one successor roster identity against the current durable process-lease head.
    ///
    /// # Errors
    /// Fails when no authority is installed or the bounded durable read is uncertain.
    pub async fn verify_current_process_incarnation(
        &self,
        participant: CheckpointParticipant,
        deadline: tokio::time::Instant,
    ) -> Result<bool, String> {
        self.process_lease_authority
            .get()
            .ok_or_else(|| "process lease authority is not installed".to_string())?
            .verify_current_participant(participant, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    /// Re-read the exact durable records behind a process fence within `deadline`.
    ///
    /// # Errors
    /// Fails closed when no shared authority is installed or verification cannot finish in time.
    pub async fn verify_process_lease_fence(
        &self,
        fence: &super::super::process_lease::ProcessLeaseFence,
        deadline: tokio::time::Instant,
    ) -> Result<bool, String> {
        self.process_lease_authority
            .get()
            .ok_or_else(|| "process lease authority is not installed".to_string())?
            .verify_fence(fence, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    /// Admit a recovery assignment only after re-verifying every durable process revocation.
    ///
    /// Keeping this operation on the controller binds the process-lease authority and leader
    /// authority installed for this runtime. Callers cannot bypass revocation verification by
    /// writing a structurally valid recovery decision directly to the leader store.
    ///
    /// # Errors
    /// Fails closed when either authority is absent, a process fence is not durable, the deadline
    /// expires, leadership changes, the decision conflicts, or storage is unavailable.
    pub async fn record_assignment_recovery_decision(
        &self,
        proof: &super::super::LeaderProof,
        decision: super::super::AssignmentRecoveryDecision,
        deadline: tokio::time::Instant,
    ) -> Result<super::super::RecordAssignmentRecoveryDecisionResult, String> {
        let process_authority = Arc::clone(
            self.process_lease_authority
                .get()
                .ok_or_else(|| "process lease authority is not installed".to_string())?,
        );
        let checks = futures::stream::iter(decision.removed_process_fences.iter().cloned())
            .map(|fence| {
                let process_authority = Arc::clone(&process_authority);
                async move { process_authority.verify_fence(&fence, deadline).await }
            })
            .buffer_unordered(CONTROL_ROSTER_IO_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        for check in checks {
            match check {
                Ok(true) => {}
                Ok(false) => {
                    return Err("assignment recovery contains an unverified process fence".into());
                }
                Err(error) => {
                    return Err(format!(
                        "assignment recovery process fence verification failed: {error}"
                    ));
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("assignment recovery authority admission exceeded its deadline".into());
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| error.to_string())?;
        Box::pin(tokio::time::timeout_at(
            deadline,
            authority.record_assignment_recovery_decision(proof, decision),
        ))
        .await
        .map_err(|_| "assignment recovery authority admission exceeded its deadline".to_string())?
        .map_err(|error| error.to_string())
    }
}
