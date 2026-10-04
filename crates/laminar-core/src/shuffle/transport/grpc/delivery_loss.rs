use super::{
    scope_cancelled_status, DeliveryTracker, Entry, FxHashMap, Ordering, PeerSeq, ShufflePeerId,
    ShuffleTopologyFence, StreamFence, Uuid,
};

impl DeliveryTracker {
    pub(super) fn reset_topology(
        &self,
        target: ShuffleTopologyFence,
        peers: &mut FxHashMap<ShufflePeerId, PeerSeq>,
    ) {
        self.topology_version
            .store(target.version(), Ordering::Release);
        peers.clear();
        self.ingress.lock().clear();
    }

    pub(super) fn topology_is_current(&self, fence: &StreamFence) -> bool {
        fence.topology.map_or(0, ShuffleTopologyFence::version)
            == self.topology_version.load(Ordering::Acquire)
    }

    /// Capture, but do not yet forgive, the cumulative loss incidents repaired by this
    /// rewind.
    pub(super) fn prepare_recovery(&self, gen: u64) {
        self.ingress.lock().clear();
        let mut pending = self.pending_recovery.lock();
        if pending.is_some_and(|(pending_gen, _)| pending_gen >= gen) {
            return;
        }
        *pending = Some((gen, self.delivery_loss_incidents.load(Ordering::Acquire)));
    }

    /// Promote only the cutoff captured for this exact recovery generation.
    /// Check a pending cutoff without promoting the repair floor or resetting loss evidence.
    /// Transport may be installed held during restore; later incidents still require a round.
    pub(super) fn recovery_covers_loss(&self, gen: u64) -> bool {
        let pending = self.pending_recovery.lock();
        let incidents = self.delivery_loss_incidents.load(Ordering::Acquire);
        incidents != u64::MAX
            && pending.is_some_and(|(generation, cutoff)| generation == gen && incidents <= cutoff)
    }

    /// Promote only the cutoff captured for this exact recovery generation.
    pub(super) fn complete_recovery(&self, gen: u64) -> bool {
        let mut pending = self.pending_recovery.lock();
        if self.completed_recovery_gen.load(Ordering::Acquire) == gen {
            return true;
        }
        let Some((pending_gen, cutoff)) = *pending else {
            return false;
        };
        if pending_gen != gen {
            return false;
        }
        // `u64::MAX` is a permanent fail-closed poison: once the incident counter is
        // exhausted, recovery must never make a later incident indistinguishable from the
        // recovered floor.
        self.recovered_delivery_loss_incidents
            .fetch_max(cutoff.min(u64::MAX - 1), Ordering::AcqRel);
        self.completed_recovery_gen.store(gen, Ordering::Release);
        *pending = None;
        true
    }

    /// Reconnects from the same process retain continuity. Process replacement is admitted
    /// only after assignment or recovery advances and opens a fresh zero-based domain.
    pub(super) fn observe_hello(&self, fence: StreamFence) -> Result<(), tonic::Status> {
        let mut peers = self.peers.lock();
        if !self.topology_is_current(&fence) {
            return Err(scope_cancelled_status());
        }
        match peers.entry(fence.sender_node_id) {
            Entry::Vacant(entry) => {
                entry.insert(PeerSeq { fence, expected: 0 });
                Ok(())
            }
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                let same_process_assignment = state.fence.sender_incarnation
                    == fence.sender_incarnation
                    && state.fence.receiver_incarnation == fence.receiver_incarnation
                    && state.fence.assignment_version == fence.assignment_version
                    && state.fence.assignment_certificate_digest
                        == fence.assignment_certificate_digest
                    && state.fence.topology == fence.topology;
                let expected =
                    if same_process_assignment && state.fence.recovery_gen == fence.recovery_gen {
                        state.expected
                    } else if fence.assignment_version > state.fence.assignment_version {
                        // Assignment publication starts a new delivery domain at sequence zero.
                        // The atomic scope check in `admit_stream` proves this is the receiver's
                        // current assignment; retaining the old map entry avoids a clear/add race.
                        0
                    } else if state.fence.topology != fence.topology {
                        return Err(tonic::Status::failed_precondition(
                            "shuffle topology changed without local installation",
                        ));
                    } else if state.fence.assignment_version == fence.assignment_version
                        && fence.recovery_gen > state.fence.recovery_gen
                    {
                        // Sender and receiver both reset their scoped sequence before admitting
                        // this generation. Starting elsewhere would hide a missing sequence zero.
                        0
                    } else {
                        return Err(tonic::Status::failed_precondition(
                            "shuffle sender scope changed without assignment or recovery advance",
                        ));
                    };
                *state = PeerSeq { fence, expected };
                Ok(())
            }
        }
    }

    pub(super) fn validate_stream(&self, fence: &StreamFence) -> Result<(), tonic::Status> {
        let peers = self.peers.lock();
        if !self.topology_is_current(fence) {
            return Err(scope_cancelled_status());
        }
        if peers
            .get(&fence.sender_node_id)
            .is_some_and(|state| state.fence == *fence)
        {
            Ok(())
        } else {
            self.note_loss(fence.sender_node_id, 1, "stale-stream");
            Err(tonic::Status::failed_precondition(
                "shuffle stream identity was superseded",
            ))
        }
    }

    /// Whether an already-enqueued frame still belongs to the current sender process and
    /// receiver scope. Reconnect stream IDs are intentionally ignored: frames queued by the
    /// predecessor connection remain ordered and valid for the same process.
    pub(super) fn matches_process_scope(
        &self,
        peer: ShufflePeerId,
        sender_incarnation: Uuid,
        receiver_incarnation: Uuid,
        assignment_version: u64,
        recovery_gen: u64,
        topology: Option<ShuffleTopologyFence>,
    ) -> bool {
        self.peers.lock().get(&peer).is_some_and(|state| {
            state.fence.sender_incarnation == sender_incarnation
                && state.fence.receiver_incarnation == receiver_incarnation
                && state.fence.assignment_version == assignment_version
                && state.fence.recovery_gen == recovery_gen
                && state.fence.topology == topology
        })
    }

    pub(super) fn note_loss(&self, peer: ShufflePeerId, missing: u64, at: &str) {
        let exhausted = self
            .delivery_loss_incidents
            .try_update(Ordering::AcqRel, Ordering::Acquire, |incidents| {
                incidents.checked_add(1)
            })
            .is_err();
        tracing::error!(
            peer,
            missing,
            at,
            loss_counter_exhausted = exhausted,
            "shuffle frames lost in transit; fencing the epoch"
        );
    }
}
