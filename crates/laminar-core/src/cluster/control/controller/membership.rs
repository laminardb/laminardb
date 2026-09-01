//! Live membership, responsiveness, draining, and recovery mode.

use super::*;

impl ClusterController {
    /// Live instance IDs: `Active` peers plus self.
    #[must_use]
    pub fn live_instances(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self
            .members_rx
            .borrow()
            .iter()
            .filter(|m| m.id != self.instance_id && matches!(m.state, NodeState::Active))
            .map(|m| m.id)
            .collect();
        if self.active.load(Ordering::SeqCst) && self.process_lease_is_live() {
            ids.push(self.instance_id);
        }
        ids
    }

    /// Nodes that must participate in a checkpoint for the current assignment.
    ///
    /// Draining owners remain responsible for their old vnodes through the handoff checkpoint,
    /// while joining, suspected, left, and missing nodes cannot contribute to a restorable cut.
    /// This is deliberately distinct from [`Self::assignable_instances`], which excludes draining
    /// nodes so they never receive new ownership.
    #[must_use]
    pub fn checkpoint_instances(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self
            .members_rx
            .borrow()
            .iter()
            .filter(|member| {
                member.id != self.instance_id
                    && matches!(member.state, NodeState::Active | NodeState::Draining)
            })
            .map(|member| member.id)
            .collect();
        if self.active.load(Ordering::SeqCst) && self.process_lease_is_live() {
            ids.push(self.instance_id);
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Record peers that failed to ack a capture quorum in time.
    pub fn note_unresponsive(&self, peers: &[NodeId]) {
        let fence = self.checkpoint_assignment_fence.borrow().clone();
        let mut map = self.unresponsive.lock();
        let mut changed = false;
        for p in peers {
            let incarnation = fence
                .as_ref()
                .and_then(|certificate| certificate.participant_incarnation(p.0));
            changed |= map.insert(p.0, incarnation) != Some(incarnation);
        }
        drop(map);
        if changed {
            self.notify_leader_eligibility_change();
        }
    }

    /// Clear peers that acked (they are demonstrably alive).
    pub fn note_responsive(&self, peers: &[NodeId]) {
        let mut map = self.unresponsive.lock();
        let mut changed = false;
        for p in peers {
            changed |= map.remove(&p.0).is_some();
        }
        drop(map);
        if changed {
            self.notify_leader_eligibility_change();
        }
    }

    /// Clear capture-quorum quarantine only for exact process boots that acknowledged a validated
    /// recovery stop round. Entries without a recorded boot, or for a different boot, remain
    /// quarantined. The caller must supply publishers from one complete, exact stopped quorum.
    pub fn note_recovery_responsive(&self, participants: &[CheckpointParticipant]) {
        let mut map = self.unresponsive.lock();
        let mut changed = false;
        for participant in participants {
            let exact = matches!(
                map.get(&participant.node_id),
                Some(Some(failed_boot)) if *failed_boot == participant.boot_incarnation
            );
            if exact {
                changed |= map.remove(&participant.node_id).is_some();
            }
        }
        drop(map);
        if changed {
            self.notify_leader_eligibility_change();
        }
    }

    /// Whether `peer` has an unresolved capture-quorum failure.
    #[must_use]
    pub fn is_unresponsive(&self, peer: NodeId) -> bool {
        self.unresponsive.lock().contains_key(&peer.0)
    }

    /// Admit a placement candidate only when it was never quarantined or it is a different boot
    /// incarnation. Callers must obtain `participant` from the lease-validated durable control KV.
    pub fn admit_successor_process(&self, participant: CheckpointParticipant) -> bool {
        let mut unresponsive = self.unresponsive.lock();
        let admitted = match unresponsive.get(&participant.node_id).copied() {
            None => true,
            Some(Some(failed_boot)) if failed_boot != participant.boot_incarnation => {
                unresponsive.remove(&participant.node_id);
                drop(unresponsive);
                self.notify_leader_eligibility_change();
                return true;
            }
            Some(Some(_) | None) => false,
        };
        drop(unresponsive);
        admitted
    }

    /// Mark this node as draining. Returns whether this exact certified leader must retain its
    /// lease long enough to coordinate the predecessor cut. Idempotent.
    pub fn begin_drain(&self) -> bool {
        #[cfg(feature = "cluster")]
        let has_durable_leadership = self.has_leader_lease_fencing();
        #[cfg(not(feature = "cluster"))]
        let has_durable_leadership = false;
        let retain_leadership = has_durable_leadership
            && self.is_leader()
            && self
                .checkpoint_assignment_fence
                .borrow()
                .as_ref()
                .is_some_and(|fence| {
                    fence.is_canonical()
                        && fence.participant_incarnation(self.instance_id.0)
                            == Some(self.recovery_incarnation)
                });
        self.draining.store(true, Ordering::SeqCst);
        if !retain_leadership && self.leader_eligible.swap(false, Ordering::SeqCst) {
            self.notify_leader_eligibility_change();
        }
        retain_leadership
    }

    /// Whether this node is draining.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// Set or clear the coordinated-recovery fence.
    pub fn set_recovering(&self, recovering: bool) {
        let _transition = self.process_authority_transition.lock();
        let recovering = recovering || !self.process_lease_is_live();
        self.recovering.store(recovering, Ordering::SeqCst);
    }

    /// Whether a coordinated restart is in flight on this node.
    #[must_use]
    pub fn is_recovering(&self) -> bool {
        self.recovering.load(Ordering::SeqCst)
    }
}
