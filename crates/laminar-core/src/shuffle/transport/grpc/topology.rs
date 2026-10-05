//! Install a logical generation using existing assignment publication and stream cancellation.

use super::*;

pub(super) fn parse_topology_fence(
    version: u64,
    digest: &[u8],
) -> Result<Option<ShuffleTopologyFence>, tonic::Status> {
    if version == 0 && digest.is_empty() {
        return Ok(None);
    }
    let digest = parse_certificate_digest(digest, "topology manifest digest")?;
    ShuffleTopologyFence::new(version, digest)
        .map(Some)
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))
}

impl ShuffleSender {
    /// Logical generation retained by this endpoint, including while assignment intake is closed.
    #[must_use]
    pub fn topology_fence(&self) -> Option<ShuffleTopologyFence> {
        self.assignment.read().as_ref().and_then(|a| a.topology)
    }
    /// Cheap graph guard. Zero denotes the legacy fabric, never an inferred topology.
    #[must_use]
    pub fn topology_version(&self) -> u64 {
        self.topology_version.load(Ordering::Acquire)
    }

    /// Fence both directions of one process's fabric to an exact committed catalog generation.
    /// The trusted caller must hold intake/execution and prove predecessor retirement and Commit.
    /// This supplies no actor readiness, output permit or cluster Release.
    ///
    /// Existing assignment locks cancel prior streams, handshakes and blocked sends before starting
    /// a new sequence domain. Identical retries preserve sequences. Process/assignment/recovery
    /// identities and unrepaired delivery-loss fences are preserved.
    ///
    /// # Errors
    /// Rejects stale/conflicting topology, incompatible endpoints, inactive assignments, expired
    /// process leases and unrepaired delivery loss before changing either direction.
    pub fn install_topology_fence_pair(
        &self,
        receiver: &ShuffleReceiver,
        expected: Option<ShuffleTopologyFence>,
        target: ShuffleTopologyFence,
    ) -> io::Result<bool> {
        self.install_topology_fence_pair_inner(receiver, expected, target, None)
    }

    /// Install held transport for an exact authorized coordinated recovery Start.
    /// The trusted caller must prove the stopped roster, selected cut and current generation.
    /// Loss covered by that generation's prepared cutoff may remain pending during installation;
    /// this does not forgive it or authorize input/output. Only recovery completion promotes the
    /// repair floor. Later loss and an exhausted incident counter still reject installation.
    ///
    /// # Errors
    /// Rejects a zero/stale generation or any ordinary process, assignment and topology mismatch.
    pub fn install_topology_fence_pair_for_recovery(
        &self,
        receiver: &ShuffleReceiver,
        expected: Option<ShuffleTopologyFence>,
        target: ShuffleTopologyFence,
        generation: u64,
    ) -> io::Result<bool> {
        if generation == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "topology recovery installation requires a nonzero generation",
            ));
        }
        self.install_topology_fence_pair_inner(receiver, expected, target, Some(generation))
    }

    fn install_topology_fence_pair_inner(
        &self,
        receiver: &ShuffleReceiver,
        expected: Option<ShuffleTopologyFence>,
        target: ShuffleTopologyFence,
        recovery: Option<u64>,
    ) -> io::Result<bool> {
        let mut sender_assignment = self.assignment.write();
        let mut receiver_assignment = receiver.assignment.write();
        self.process_lease.require_live_io()?;
        receiver.process_lease.require_live_io()?;
        let sender = sender_assignment.as_ref().ok_or_else(scope_cancelled_io)?;
        let inbound = receiver_assignment
            .as_ref()
            .ok_or_else(scope_cancelled_io)?;
        let mut delivery_peers = receiver.delivery.peers.lock();
        if self.local_id != receiver.local_id
            || self.sender_incarnation != receiver.receiver_incarnation
            || sender.fence != inbound.fence
            || sender.digest != inbound.digest
            || sender.owners != inbound.owners
            || sender.topology != inbound.topology
            || self.assignment_version.load(Ordering::Acquire) != sender.fence.assignment_version
            || receiver.assignment_version.load(Ordering::Acquire)
                != inbound.fence.assignment_version
            || self.recovery_gen.load(Ordering::Acquire)
                != receiver.recovery_gen.load(Ordering::Acquire)
            || self.scope_cancel.read().is_cancelled()
            || receiver.scope_cancel.read().is_cancelled()
            || recovery.is_some_and(|generation| {
                receiver.recovery_gen.load(Ordering::Acquire) != generation
            })
            || (receiver.has_unrecovered_delivery_loss()
                && recovery
                    .is_none_or(|generation| !receiver.delivery.recovery_covers_loss(generation)))
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "topology installation requires the exact live local shuffle fabric",
            ));
        }
        if sender.topology == Some(target) {
            return Ok(false);
        }
        if sender.topology != expected
            || expected.is_some_and(|old| target.version() <= old.version())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shuffle topology conflicts with its retained generation",
            ));
        }
        let next = |old: &InstalledAssignment| {
            Arc::new(InstalledAssignment {
                fence: old.fence.clone(),
                digest: old.digest,
                owners: Arc::clone(&old.owners),
                topology: Some(target),
            })
        };
        let next_sender = next(sender);
        let next_receiver = next(inbound);
        rotate_scope_token(&self.scope_cancel, &self.process_lease, false);
        rotate_scope_token(&receiver.scope_cancel, &receiver.process_lease, false);
        self.pool.lock().clear();
        self.seqs.lock().clear();
        self.connect_locks.lock().clear();
        receiver.pending_handshakes.clear();
        receiver
            .delivery
            .reset_topology(target, &mut delivery_peers);
        receiver.holdover.clear_staged_frontiers();
        let barriers = receiver.holdover.take_staged_barriers();
        receiver.holdover.release_items(barriers.len());
        drop(barriers);
        *sender_assignment = Some(next_sender);
        *receiver_assignment = Some(next_receiver);
        self.topology_version
            .store(target.version(), Ordering::Release);
        receiver
            .topology_version
            .store(target.version(), Ordering::Release);
        rotate_scope_token(&self.scope_cancel, &self.process_lease, true);
        rotate_scope_token(&receiver.scope_cancel, &receiver.process_lease, true);
        receiver.work_ready.notify_waiters();
        receiver.assignment_resumed.notify_waiters();
        Ok(true)
    }

    /// Send under an immutable graph topology binding and exact routing assignment.
    /// Existing unbound send methods target only the legacy fabric.
    /// # Errors
    /// Rejects stale topology before sequence allocation, as well as ordinary send failures.
    pub async fn send_to_for_topology(
        &self,
        peer: ShufflePeerId,
        expected_assignment_version: u64,
        topology: Option<ShuffleTopologyFence>,
        message: &ShuffleMessage,
    ) -> io::Result<()> {
        self.send_to_inner(
            peer,
            message,
            Some(expected_assignment_version),
            None,
            topology,
            None,
        )
        .await
    }

    /// Send under the exact routing assignment, recovery generation, and graph topology.
    /// A retained pre-recovery plan cannot acquire a stream in a newer recovery generation.
    ///
    /// # Errors
    /// Rejects a stale generation before connection or sequence allocation, plus ordinary send errors.
    pub async fn send_to_for_generation(
        &self,
        peer: ShufflePeerId,
        assignment_version: u64,
        recovery_generation: u64,
        topology: Option<ShuffleTopologyFence>,
        message: &ShuffleMessage,
    ) -> io::Result<()> {
        self.send_to_inner(
            peer,
            message,
            Some(assignment_version),
            None,
            topology,
            Some(recovery_generation),
        )
        .await
    }

    pub(super) fn current_send_scope(
        &self,
        assignment: Option<u64>,
        topology: Option<ShuffleTopologyFence>,
        recovery: Option<u64>,
    ) -> io::Result<ScopeLease> {
        let scope = self.current_scope(assignment, topology)?;
        if recovery.is_some_and(|expected| expected != scope.recovery_gen) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "shuffle send plan belongs to a stale recovery generation",
            ));
        }
        Ok(scope)
    }
}

pub(super) fn outbound_admission_bytes(
    message: &ShuffleMessage,
    assignment_fence: Option<&CheckpointAssignmentFence>,
) -> io::Result<usize> {
    let bytes = outbound_workspace_bytes(message)?;
    if matches!(message, ShuffleMessage::Barrier(_)) && assignment_fence.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "shuffle checkpoint barriers require an admitted assignment certificate",
        ));
    }
    Ok(bytes)
}

impl ShuffleReceiver {
    /// Logical generation retained by this endpoint, including while assignment intake is closed.
    #[must_use]
    pub fn topology_fence(&self) -> Option<ShuffleTopologyFence> {
        self.assignment.read().as_ref().and_then(|a| a.topology)
    }
    /// Cheap graph guard. Zero denotes the legacy fabric, never an inferred topology.
    #[must_use]
    pub fn topology_version(&self) -> u64 {
        self.topology_version.load(Ordering::Acquire)
    }
}

impl ShuffleSender {
    pub(super) fn current_scope(
        &self,
        expected: Option<u64>,
        topology: Option<ShuffleTopologyFence>,
    ) -> io::Result<ScopeLease> {
        let assignment = self.assignment.read();
        let installed = assignment.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "shuffle assignment certificate is not installed",
            )
        })?;
        let version = self.assignment_version.load(Ordering::Acquire);
        let recovery_gen = self.recovery_gen.load(Ordering::Acquire);
        let cancel = self.scope_cancel.read().clone();
        self.process_lease.require_live_io()?;
        if version == 0
            || version != installed.fence.assignment_version
            || cancel.is_cancelled()
            || installed.topology != topology
        {
            return Err(scope_cancelled_io());
        }
        if expected.is_some_and(|expected| expected == 0 || expected != version) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                format!(
                    "shuffle assignment scope mismatch: routed at {}, sender at {version}",
                    expected.unwrap_or_default()
                ),
            ));
        }
        Ok(ScopeLease {
            assignment: Arc::clone(installed),
            recovery_gen,
            cancel,
        })
    }

    pub(super) fn validate_expected_assignment(&self, expected: Option<u64>) -> io::Result<()> {
        let Some(expected) = expected else {
            return Ok(());
        };
        let current = self.assignment_version.load(Ordering::Acquire);
        if expected != 0 && expected == current {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            format!("shuffle assignment scope mismatch: routed at {expected}, sender at {current}"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topology_transport_rejects_partial_malformed_and_zero_wire_identities() {
        assert_eq!(parse_topology_fence(0, &[]).unwrap(), None);
        for (version, digest) in [
            (0, vec![1; 32]),
            (1, vec![]),
            (1, vec![0; 32]),
            (1, vec![1; 31]),
            (1, vec![1; 33]),
        ] {
            assert!(parse_topology_fence(version, &digest).is_err());
        }
        let target = ShuffleTopologyFence::new(2, [42; 32]).unwrap();
        assert_eq!(parse_topology_fence(2, &[42; 32]).unwrap(), Some(target));
        assert!(ShuffleTopologyFence::new(0, [42; 32]).is_err());
    }

    #[test]
    fn topology_transport_late_old_admission_cannot_fault_or_advance_target_delivery() {
        let delivery = DeliveryTracker::default();
        let original = StreamFence {
            sender_node_id: 1,
            sender_incarnation: Uuid::from_u128(1),
            receiver_incarnation: Uuid::from_u128(2),
            stream_id: Uuid::from_u128(3),
            assignment_version: 1,
            assignment_certificate_digest: [1; 32],
            recovery_gen: 0,
            topology: None,
        };
        delivery.observe_hello(original).unwrap();
        let pending = delivery.prepare_data(&original, 0).unwrap().unwrap();
        let barrier = delivery.prepare_barrier(&original, 1).unwrap();
        let topology = ShuffleTopologyFence::new(2, [42; 32]).unwrap();
        delivery.reset_topology(topology, &mut delivery.peers.lock());
        assert_eq!(
            delivery.commit_data(pending).unwrap_err().code(),
            tonic::Code::Cancelled
        );
        assert_eq!(
            delivery.commit_barrier(barrier).unwrap_err().code(),
            tonic::Code::Cancelled
        );
        assert_eq!(
            delivery.validate_stream(&original).unwrap_err().code(),
            tonic::Code::Cancelled
        );
        assert_eq!(
            delivery.prepare_barrier(&original, 1).err().unwrap().code(),
            tonic::Code::Cancelled
        );
        let current = StreamFence {
            topology: Some(topology),
            stream_id: Uuid::from_u128(4),
            ..original
        };
        delivery.observe_hello(current).unwrap();
        let pending = delivery.prepare_data(&current, 0).unwrap().unwrap();
        delivery.commit_data(pending).unwrap();
        assert_eq!(delivery.delivery_loss_incidents.load(Ordering::Acquire), 0);
        assert_eq!(delivery.peers.lock()[&1].expected, 1);
    }
}
