//! Assignable membership, drain acknowledgements, assignment adoption, and leader audit.

use super::*;

impl ClusterController {
    /// Node ids eligible to own vnodes: `Active` peers, plus self unless draining. Unlike
    /// [`Self::live_instances`], non-`Active` peers are filtered so they never receive vnodes.
    #[must_use]
    pub fn assignable_instances(&self) -> Vec<NodeId> {
        let mut ids = assignable_node_ids(&self.members_rx.borrow());
        ids.retain(|id| *id != self.instance_id);
        if self.active.load(Ordering::SeqCst)
            && !self.is_draining()
            && !self.instance_id.is_unassigned()
        {
            ids.push(self.instance_id);
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Record this node's own locality. Call once at startup.
    pub fn set_self_locality(&self, locality: Locality) {
        *self.self_locality.write() = locality;
    }

    /// [`Self::assignable_instances`] paired with each node's [`Locality`]
    /// (peers' from `members_rx`, self's from [`Self::set_self_locality`]).
    #[must_use]
    pub fn assignable_with_locality(&self) -> Vec<(NodeId, Locality)> {
        let members = self.members_rx.borrow();
        self.assignable_instances()
            .into_iter()
            .map(|id| {
                let locality = if id == self.instance_id {
                    self.self_locality.read().clone()
                } else {
                    members
                        .iter()
                        .find(|m| m.id == id)
                        .and_then(|m| m.metadata.failure_domain.as_deref())
                        .map(Locality::parse)
                        .unwrap_or_default()
                };
                (id, locality)
            })
            .collect()
    }

    /// Cloneable membership watch for reacting to join/leave events without polling.
    #[must_use]
    pub fn members_watch(&self) -> watch::Receiver<Vec<NodeInfo>> {
        self.members_rx.clone()
    }

    pub(super) async fn drain_transition_authority_is_current(
        &self,
        transition: &AssignmentDrainTransition,
    ) -> Result<bool, String> {
        #[cfg(feature = "cluster")]
        {
            let authority = self
                .checkpoint_authority()
                .map_err(|error| error.to_string())?;
            let Some(lease) = authority.load().await.map_err(|error| error.to_string())? else {
                return Ok(false);
            };
            Ok(lease.matches_proof(&transition.leader))
        }
        #[cfg(not(feature = "cluster"))]
        {
            let _ = transition;
            Err("assignment drain authority requires the cluster feature".into())
        }
    }

    /// Publish and exactly read back proof that every local source reached one exact external
    /// input cut for `transition`.
    ///
    /// # Errors
    /// Rejects a nonparticipant, stale boot, superseded process, malformed certificate, or failed
    /// durable write/read-back.
    pub async fn announce_drain_ack(
        &self,
        transition: &AssignmentDrainTransition,
    ) -> Result<(), String> {
        if !transition.is_canonical() {
            return Err("drain acknowledgement requires an exact assignment transition".into());
        }
        let participant = CheckpointParticipant {
            node_id: self.instance_id.0,
            boot_incarnation: self.recovery_incarnation,
        };
        if transition
            .predecessor
            .participant_incarnation(participant.node_id)
            != Some(participant.boot_incarnation)
        {
            return Err("drain acknowledgement does not bind the local process".into());
        }
        if self.checkpoint_drain_transition.borrow().as_ref() != Some(transition) {
            return Err("drain acknowledgement is not the locally audited transition".into());
        }
        if !self.process_lease_is_live()
            || !self
                .drain_transition_authority_is_current(transition)
                .await?
        {
            return Err("drain acknowledgement authority is no longer live".into());
        }
        if !self.recovery_incarnation_is_current().await? {
            return Err("drain acknowledgement came from a superseded local process".into());
        }
        self.write_recovery_value_exact(
            DRAIN_ACK_KEY,
            encode_drain_ack(&DrainAck::for_transition(participant, transition))?,
        )
        .await
    }

    /// Whether every process in the exact frozen assignment roster durably acknowledged the same
    /// certificate and the transition remains the exact unfinalized durable assignment head.
    ///
    /// Records from nodes outside `fence` are ignored because durable per-node slots can outlive
    /// membership. A missing or different-version record never contributes to quorum.
    ///
    /// # Errors
    /// Fails closed when the certificate, current boot roster, assignment head, terminal decision,
    /// durable scan, or an expected participant's record is malformed or unavailable. A
    /// controller without a configured assignment store returns `Ok(false)` while a drain is
    /// active.
    pub async fn drain_ack_quorum_reached(
        &self,
        transition: &AssignmentDrainTransition,
    ) -> Result<bool, String> {
        if !transition.is_canonical() {
            return Err("drain quorum requires a canonical assignment transition".into());
        }
        let locally_audited =
            self.checkpoint_drain_transition.borrow().as_ref() == Some(transition);
        if !locally_audited
            || !self
                .drain_transition_authority_is_current(transition)
                .await?
        {
            return Ok(false);
        }
        if self
            .recovery_participant_incarnations(&transition.predecessor.participant_ids())
            .await?
            != transition.predecessor.participants
        {
            return Ok(false);
        }

        let expected: std::collections::BTreeSet<NodeId> = transition
            .required_participants()
            .iter()
            .map(|participant| NodeId(participant.node_id))
            .collect();
        let mut seen = std::collections::BTreeSet::new();
        let mut matching = std::collections::BTreeSet::new();
        for (publisher, raw) in self.scan_recovery_values(DRAIN_ACK_KEY).await? {
            if !expected.contains(&publisher) {
                continue;
            }
            if !seen.insert(publisher) {
                return Err(format!(
                    "duplicate drain acknowledgement from expected participant {publisher}"
                ));
            }
            let ack = parse_drain_ack(&raw, publisher)?;
            if ack.matches_transition(transition) {
                matching.insert(publisher);
            }
        }
        if matching != expected {
            return Ok(false);
        }
        // A complete receipt roster is not itself permission to publish a handoff checkpoint.
        // The drain may already have been terminalized (or a newer assignment may have become
        // the durable head) while those immutable per-process receipts remain visible.
        let Some(snapshot_store) = self.snapshot.as_ref() else {
            return Ok(false);
        };
        let Some(head) = snapshot_store
            .load()
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(false);
        };
        if !head.draining
            || head.drain_transition.as_ref() != Some(transition)
            || head.assignment_fence().map_err(|error| error.to_string())? != transition.target
        {
            return Ok(false);
        }
        // Close the read/read race: a process can restart after the first incarnation scan but
        // before its old acknowledgement is observed. The drain head can likewise be finalized
        // during the durable reads. Revalidate all process-local authority after the exact cut;
        // callers retain the transition and perform their own post-I/O check before publication.
        if self
            .recovery_participant_incarnations(&transition.predecessor.participant_ids())
            .await?
            != transition.predecessor.participants
            || !self
                .drain_transition_authority_is_current(transition)
                .await?
        {
            return Ok(false);
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| error.to_string())?;
        if authority
            .assignment_drain_decision(transition.target.assignment_version)
            .await
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Ok(false);
        }
        Ok(
            self.checkpoint_drain_transition.borrow().as_ref() == Some(transition)
                && self.process_lease_is_live(),
        )
    }

    /// Publish and exactly read back this process's adopted assignment map.
    ///
    /// # Errors
    /// Fails closed for a malformed/stale process report or unavailable control storage.
    pub async fn announce_adopted_assignment(
        &self,
        adoption: &CheckpointAssignmentAdoption,
    ) -> Result<(), String> {
        if !adoption.is_canonical()
            || adoption.participant.node_id != self.instance_id.0
            || adoption.participant.boot_incarnation != self.recovery_incarnation
            || !self.recovery_incarnation_is_current().await?
        {
            return Err("adopted assignment report does not bind the current process".into());
        }
        let encoded = serde_json::to_string(adoption)
            .map_err(|error| format!("could not encode adopted assignment: {error}"))?;
        self.write_recovery_value_exact(ADOPTED_ASSIGNMENT_KEY, encoded)
            .await
    }

    /// Each visible process's exact adopted assignment identity.
    ///
    /// # Errors
    /// Fails closed if any visible report is malformed or claims another publisher.
    pub async fn read_adopted_assignments(
        &self,
    ) -> Result<Vec<(NodeId, CheckpointAssignmentAdoption)>, String> {
        self.scan_recovery_values(ADOPTED_ASSIGNMENT_KEY)
            .await?
            .into_iter()
            .map(|(node, raw)| {
                let adoption: CheckpointAssignmentAdoption = serde_json::from_str(&raw)
                    .map_err(|error| format!("invalid adopted assignment from {node}: {error}"))?;
                if !adoption.is_canonical() || adoption.participant.node_id != node.0 {
                    return Err(format!(
                        "adopted assignment from {node} has a non-canonical publisher"
                    ));
                }
                Ok((node, adoption))
            })
            .collect()
    }

    /// Publish this node's version-bound checkpoint fence. Called off the hot path by the snapshot
    /// watcher; admission and recovery revalidate it locally against current membership.
    pub fn publish_checkpoint_assignment_fence(&self, fence: Option<CheckpointAssignmentFence>) {
        let _transition = self.process_authority_transition.lock();
        let fence = if self.process_lease_is_live() {
            fence
        } else {
            None
        };
        if let Some(fence) = fence.as_ref().filter(|fence| fence.is_canonical()) {
            let participants = fence.participant_ids();
            let changed = {
                let mut installed = self.leadership_participants.write();
                match installed.as_ref() {
                    Some((version, _)) if *version >= fence.assignment_version => false,
                    _ => {
                        *installed = Some((fence.assignment_version, participants));
                        true
                    }
                }
            };
            if changed {
                self.notify_leader_eligibility_change();
            }
        }
        self.checkpoint_assignment_fence.send_replace(fence);
    }

    /// Publish the exact locally audited drain transition, or clear it after commit/abort.
    pub fn publish_checkpoint_drain_transition(
        &self,
        transition: Option<AssignmentDrainTransition>,
    ) {
        let _transition = self.process_authority_transition.lock();
        let transition = if self.process_lease_is_live() {
            transition
        } else {
            None
        };
        self.checkpoint_drain_transition.send_replace(transition);
    }

    /// Clear the locally audited drain transition only when it still equals `expected`.
    ///
    /// The comparison and clear share the process-authority transition lock with publication and
    /// terminal process fencing, so a newer transition cannot be erased by stale reconciliation.
    #[must_use]
    pub fn clear_checkpoint_drain_transition_if_matches(
        &self,
        expected: &AssignmentDrainTransition,
    ) -> bool {
        let _transition = self.process_authority_transition.lock();
        let matches = self.checkpoint_drain_transition.borrow().as_ref() == Some(expected);
        if matches {
            self.checkpoint_drain_transition.send_replace(None);
        }
        matches
    }

    /// Current locally audited drain transition.
    #[must_use]
    pub fn checkpoint_drain_transition(&self) -> Option<AssignmentDrainTransition> {
        self.checkpoint_drain_transition.borrow().clone()
    }

    /// Subscribe to local assignment-fence changes so a blocking durability probe can be
    /// interrupted when its checkpoint cut becomes stale.
    #[must_use]
    pub fn checkpoint_assignment_watch(
        &self,
    ) -> watch::Receiver<Option<CheckpointAssignmentFence>> {
        self.checkpoint_assignment_fence.subscribe()
    }

    /// Return the exact locally certified assignment cut while every participant remains
    /// checkpoint-capable. Active workers that own no vnode do not expand the checkpoint quorum.
    /// The clone is retained by the admitted attempt and propagated to followers.
    #[must_use]
    pub fn checkpoint_assignment_fence(
        &self,
        assignment_version: u64,
    ) -> Option<CheckpointAssignmentFence> {
        let checkpoint_capable: Vec<u64> = self
            .checkpoint_instances()
            .into_iter()
            .map(|node| node.0)
            .collect();
        self.checkpoint_assignment_fence
            .borrow()
            .as_ref()
            .filter(|fence| {
                fence.is_canonical()
                    && fence.assignment_version == assignment_version
                    && fence
                        .participant_incarnation(self.instance_id.0)
                        .is_none_or(|incarnation| incarnation == self.recovery_incarnation)
                    && fence.participants.iter().all(|participant| {
                        checkpoint_capable
                            .binary_search(&participant.node_id)
                            .is_ok()
                    })
            })
            .cloned()
    }

    /// Return the locally certified assignment only when the announcing leader proof exactly
    /// matches the current durable authority. A predecessor retained during an in-progress drain
    /// additionally remains bound to the leader term that opened that drain.
    #[cfg(feature = "cluster")]
    pub async fn checkpoint_assignment_fence_for_leader(
        &self,
        assignment_version: u64,
        leader: &crate::checkpoint::LeaderProof,
    ) -> Option<CheckpointAssignmentFence> {
        if !leader.is_canonical() {
            return None;
        }
        let fence = self.checkpoint_assignment_fence(assignment_version)?;
        let authority = self.checkpoint_authority().ok()?;
        let lease = authority.load().await.ok().flatten()?;
        if !lease.matches_proof(leader) {
            return None;
        }
        self.checkpoint_assignment_fence_after_authority_validation(fence, leader)
    }

    #[cfg(feature = "cluster")]
    pub(super) fn checkpoint_assignment_fence_after_authority_validation(
        &self,
        fence: CheckpointAssignmentFence,
        leader: &crate::checkpoint::LeaderProof,
    ) -> Option<CheckpointAssignmentFence> {
        let transition = self.checkpoint_drain_transition.borrow();
        if transition
            .as_ref()
            .is_some_and(|transition| transition.predecessor == fence)
            && transition.as_ref().map(|transition| &transition.leader) != Some(leader)
        {
            return None;
        }
        Some(fence)
    }

    /// Start the direct gRPC barrier sync server.
    ///
    /// # Errors
    /// Propagates [`BarrierCoordinator::start_server`] errors.
    #[cfg(feature = "cluster")]
    pub async fn start_barrier_server(
        &self,
        bind_addr: std::net::SocketAddr,
        advertise_host: Option<String>,
    ) -> Result<std::net::SocketAddr, String> {
        self.barrier.start_server(bind_addr, advertise_host).await
    }

    /// Start a cluster control endpoint whose first advertisement is bound to an acquired
    /// stable-node process lease.
    ///
    /// # Errors
    /// Rejects a lease for another node or boot, a conflicting prior identity, or server start.
    #[cfg(feature = "cluster")]
    pub async fn start_leased_barrier_server(
        &self,
        bind_addr: std::net::SocketAddr,
        advertise_host: Option<String>,
        process_lease: &super::super::ProcessLease,
    ) -> Result<std::net::SocketAddr, String> {
        process_lease
            .validate(self.instance_id)
            .map_err(|error| error.to_string())?;
        if process_lease.owner != self.recovery_incarnation {
            return Err("control endpoint lease does not bind this process incarnation".into());
        }
        if !self.recovery_process_lease_is_live() {
            return Err("live process lease deadline is not installed".into());
        }
        self.barrier.install_local_process_lease(process_lease)?;
        self.barrier.start_server(bind_addr, advertise_host).await
    }

    /// Confirm that one exact remote process still holds a proof read from durable authority.
    ///
    /// The RPC returns only a challenge acknowledgement and never returns authority material.
    ///
    /// # Errors
    /// Fails closed when the control RPC is unavailable, malformed, or misses `deadline`.
    #[cfg(feature = "cluster")]
    pub async fn confirm_remote_leader_proof(
        &self,
        proof: &LeaderProof,
        deadline: tokio::time::Instant,
    ) -> Result<bool, String> {
        self.barrier
            .confirm_remote_leader_proof(proof, deadline)
            .await
    }

    #[cfg(feature = "cluster")]
    pub(super) async fn confirm_assignment_leader_proof(
        &self,
        leader: NodeId,
        proof: &LeaderProof,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        if tokio::time::Instant::now() >= deadline {
            return Err("assignment leader authority audit deadline expired".into());
        }
        if proof.owner.node_id != leader.0 {
            return Err("assignment leader proof does not match the current leader".into());
        }
        let confirmed = if leader == self.instance_id {
            self.capture_leader_proof().as_ref() == Some(proof)
        } else {
            self.confirm_remote_leader_proof(proof, deadline).await?
        };
        if !confirmed {
            return Err(format!(
                "assignment leader process {} has no live exact grant",
                leader.0
            ));
        }
        Ok(())
    }

    /// Audit one exact assignment leader against durable and process-local authority.
    ///
    /// This is a single bounded attempt intended for a caller that already holds its assignment
    /// and execution locks. `expected_proof` binds predecessor execution during an active drain;
    /// a stable assignment passes `None` and adopts the current durable grant. The audit never
    /// waits for convergence and never mutates assignment, lease, or source-gate state.
    ///
    /// # Errors
    /// Fails closed when the installed assignment, elected participant, leader authority, live
    /// local/remote grant, or durable process term changes or cannot be verified before `deadline`.
    #[cfg(feature = "cluster")]
    pub async fn audit_assignment_leader_authority(
        &self,
        fence: &CheckpointAssignmentFence,
        expected_proof: Option<&LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<LeaderProof, String> {
        if tokio::time::Instant::now() >= deadline {
            return Err("assignment leader authority audit deadline expired".into());
        }
        if !fence.is_canonical()
            || self
                .checkpoint_assignment_fence(fence.assignment_version)
                .as_ref()
                != Some(fence)
        {
            return Err(
                "assignment leader authority audit requires the exact installed fence".into(),
            );
        }
        if expected_proof.is_some_and(|proof| !proof.is_canonical()) {
            return Err("assignment leader authority audit expected proof is not canonical".into());
        }

        let leader = self
            .current_leader()
            .ok_or_else(|| "assignment leader authority audit has no current leader".to_string())?;
        let participant = fence
            .participants
            .iter()
            .find(|participant| participant.node_id == leader.0)
            .copied()
            .ok_or_else(|| {
                "current leader is not a participant in the exact assignment fence".to_string()
            })?;
        let authority = self
            .checkpoint_authority()
            .map_err(|error| format!("assignment leader authority is unavailable: {error}"))?;
        let initial = tokio::time::timeout_at(deadline, authority.load())
            .await
            .map_err(|_| "assignment leader authority initial read timed out".to_string())?
            .map_err(|error| format!("assignment leader authority initial read failed: {error}"))?
            .ok_or_else(|| "assignment leader authority has no durable grant".to_string())?;
        if initial.owner.node != leader || initial.owner.boot != participant.boot_incarnation {
            return Err(
                "durable leader grant does not match the elected assignment participant".into(),
            );
        }
        let proof = match expected_proof {
            Some(expected) if initial.matches_proof(expected) => expected.clone(),
            Some(_) => {
                return Err(
                    "durable leader grant does not match the drain-bound expected proof".into(),
                );
            }
            None => initial.proof(),
        };

        self.confirm_assignment_leader_proof(leader, &proof, deadline)
            .await?;
        let process_authority = self
            .process_lease_authority
            .get()
            .ok_or_else(|| "process lease authority is not installed".to_string())?;
        let process_current = process_authority
            .verify_current_participant_term(participant, proof.owner.process_term, deadline)
            .await
            .map_err(|error| {
                format!("assignment leader process-term verification failed: {error}")
            })?;
        if !process_current {
            return Err("assignment leader durable process term is no longer current".to_string());
        }
        self.confirm_assignment_leader_proof(leader, &proof, deadline)
            .await?;

        let final_grant = tokio::time::timeout_at(deadline, authority.load())
            .await
            .map_err(|_| "assignment leader authority final read timed out".to_string())?
            .map_err(|error| format!("assignment leader authority final read failed: {error}"))?
            .ok_or_else(|| "assignment leader authority grant disappeared".to_string())?;
        if final_grant.owner != initial.owner
            || final_grant.token != initial.token
            || final_grant.seq < initial.seq
            || !final_grant.matches_proof(&proof)
        {
            return Err("assignment leader durable grant changed during the audit".into());
        }
        if self.current_leader() != Some(leader)
            || self
                .checkpoint_assignment_fence(fence.assignment_version)
                .as_ref()
                != Some(fence)
        {
            return Err("assignment leader or exact fence changed during the audit".into());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("assignment leader authority audit deadline expired".into());
        }
        Ok(proof)
    }
}
