//! Durable intent and materialization of an exact graceful assignment drain.

use super::topology_admission::MAX_ADMISSION_ATTEMPTS;
use super::*;
use crate::cluster::control::snapshot::AssignmentSnapshot;

const DRAIN_PROPOSAL_PREFIX: &str = "control/assignment-drain-proposals/v1/";

fn drain_path(reference: &AssignmentSnapshotRef) -> OsPath {
    OsPath::from(format!(
        "{DRAIN_PROPOSAL_PREFIX}v{:020}/{}.json",
        reference.version, reference.sha256
    ))
}

impl LeaderLeaseStore {
    async fn load_drain_proposal(
        &self,
        reference: &AssignmentSnapshotRef,
    ) -> Result<AssignmentSnapshot, LeaseError> {
        reference
            .validate()
            .map_err(|e| LeaseError::Invalid(e.to_string()))?;
        let bytes = self
            .read_admission_blob(&drain_path(reference), reference.encoded_len)
            .await?;
        let proposal: AssignmentSnapshot =
            serde_json::from_slice(&bytes).map_err(|e| LeaseError::Invalid(e.to_string()))?;
        let (canonical, actual) = proposal
            .encode_drain_proposal()
            .map_err(|e| LeaseError::Invalid(e.to_string()))?;
        if actual != *reference || canonical.as_slice() != bytes.as_ref() {
            return Err(LeaseError::Invalid(
                "drain proposal does not match its immutable reference".into(),
            ));
        }
        Ok(proposal)
    }

    /// Reserve and publish an exact graceful assignment drain through shared authority.
    ///
    /// A reservation survives cancellation before snapshot publication. Checkpoint/migration
    /// admission sees that reservation; only an exact drain/recovery decision can release it.
    /// The supplied store must be the namespace-verified assignment store used by the controller.
    ///
    /// # Errors
    /// Rejects stale authority, another reserved operation, a changed predecessor, or storage I/O.
    pub async fn publish_assignment_drain(
        &self,
        proof: &LeaderProof,
        assignments: &AssignmentSnapshotStore,
        proposal: &AssignmentSnapshot,
    ) -> Result<RotateOutcome, ClusterCheckpointAuthorityError> {
        let (encoded, reference) = proposal
            .encode_drain_proposal()
            .map_err(snapshot_authority_error)?;
        let transition = proposal
            .drain_transition
            .as_ref()
            .ok_or_else(|| LeaseError::Invalid("missing drain transition".into()))?;
        if &transition.leader != proof || !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        for _ in 0..MAX_ADMISSION_ATTEMPTS {
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &published.record;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            current.reject_topology_preparation()?;
            self.validate_topology_assignment_proposal_from(current, &transition.target)
                .await?;
            if let Some(reservation) = &current.assignment_drain_reservation {
                if reservation.proposal != reference || &reservation.transition != transition {
                    return Err(DecisionError::Conflict(
                        "another assignment drain is reserved".into(),
                    )
                    .into());
                }
                self.load_drain_proposal(&reference).await?;
                return assignments
                    .save_if_version(proposal, transition.predecessor.assignment_version)
                    .await
                    .map_err(snapshot_authority_error);
            }
            if current
                .recovery_fault_slots
                .iter()
                .any(|slot| slot.active || slot.disposition == RecoveryFaultDisposition::Terminal)
            {
                return Err(DecisionError::Conflict(
                    "assignment drain cannot overtake recovery".into(),
                )
                .into());
            }
            let predecessor = assignments
                .load()
                .await
                .map_err(snapshot_authority_error)?
                .ok_or_else(|| LeaseError::Invalid("assignment predecessor is missing".into()))?;
            if predecessor.version == proposal.version {
                // Compatibility with a drain published before this binary's coordinated upgrade.
                // Definitive settlement still uses the shared authority as before.
                return Ok(RotateOutcome::Conflict(Box::new(predecessor)));
            }
            if predecessor.draining
                || predecessor
                    .assignment_fence()
                    .map_err(snapshot_authority_error)?
                    != transition.predecessor
            {
                return Err(
                    DecisionError::Conflict("assignment drain predecessor changed".into()).into(),
                );
            }
            self.reject_consumed_checkpoint_assignment(current, &transition.predecessor)
                .await?;
            self.stage_admission_blob(&drain_path(&reference), &encoded)
                .await?;
            let mut lease = current.lease.clone();
            lease.seq = lease
                .seq
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("authority sequence exhausted".into()))?;
            let sequence = lease.seq;
            let mut next = current.preserve_with_lease(lease);
            next.version = next.version.max(TOPOLOGY_ADMISSION_RECORD_VERSION);
            next.assignment_drain_reservation = Some(AssignmentDrainReservation {
                proposal: reference.clone(),
                transition: transition.clone(),
                authority_sequence: sequence,
            });
            match self
                .create_authority_record(Some(&published), &next)
                .await?
            {
                AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                    return assignments
                        .save_if_version(proposal, transition.predecessor.assignment_version)
                        .await
                        .map_err(snapshot_authority_error);
                }
                AuthorityCreateOutcome::Contended(_) => tokio::task::yield_now().await,
            }
        }
        Err(LeaseError::Io(
            "assignment drain admission contention exhausted its fixed retry bound".into(),
        )
        .into())
    }

    /// Materialize an already-admitted drain after cancellation or leader replacement.
    ///
    /// This publishes the original fenced intent; it never transfers vnode ownership or reopens
    /// intake. A replacement leader settles it through the existing drain/recovery decision path.
    ///
    /// # Errors
    /// Rejects corrupt/missing proposals or conflicting assignment history.
    pub async fn materialize_reserved_assignment_drain(
        &self,
        assignments: &AssignmentSnapshotStore,
    ) -> Result<Option<RotateOutcome>, ClusterCheckpointAuthorityError> {
        let Some(head) = self.load_record().await? else {
            return Ok(None);
        };
        let Some(reservation) = &head.assignment_drain_reservation else {
            return Ok(None);
        };
        self.materialize_drain_reservation(reservation, assignments)
            .await
            .map(Some)
    }

    pub(super) async fn materialize_drain_reservation(
        &self,
        reservation: &AssignmentDrainReservation,
        assignments: &AssignmentSnapshotStore,
    ) -> Result<RotateOutcome, ClusterCheckpointAuthorityError> {
        let anchor = read_authority_record(self.store.as_ref(), reservation.authority_sequence)
            .await?
            .ok_or_else(|| {
                LeaseError::Invalid("assignment reservation authority anchor is missing".into())
            })?;
        if anchor.assignment_drain_reservation.as_ref() != Some(reservation) {
            return Err(LeaseError::Invalid(
                "assignment reservation differs from its authority anchor".into(),
            )
            .into());
        }
        let proposal = self.load_drain_proposal(&reservation.proposal).await?;
        if proposal.drain_transition.as_ref() != Some(&reservation.transition) {
            return Err(LeaseError::Invalid(
                "reserved drain transition differs from its proposal".into(),
            )
            .into());
        }
        let outcome = assignments
            .save_if_version(
                &proposal,
                reservation.transition.predecessor.assignment_version,
            )
            .await
            .map_err(snapshot_authority_error)?;
        if matches!(&outcome, RotateOutcome::Conflict(winner) if winner.draining && winner.as_ref() != &proposal)
        {
            return Err(LeaseError::Invalid(
                "reserved drain lost to a different drain proposal".into(),
            )
            .into());
        }
        Ok(outcome)
    }

    pub(super) async fn prune_drain_proposals(&self, before: u64) -> Result<(), LeaseError> {
        if before == 0 {
            return Ok(());
        }
        let prefix = OsPath::from(DRAIN_PROPOSAL_PREFIX);
        let mut objects = self.store.list(Some(&prefix));
        let mut candidates = Vec::new();
        let mut scanned = 0;
        while let Some(object) = objects.next().await {
            let object = object.map_err(|e| LeaseError::Io(e.to_string()))?;
            let suffix = object
                .location
                .as_ref()
                .strip_prefix(DRAIN_PROPOSAL_PREFIX)
                .ok_or_else(|| {
                    LeaseError::Invalid("drain proposal escaped its namespace".into())
                })?;
            let (version, digest) = suffix
                .split_once('/')
                .ok_or_else(|| LeaseError::Invalid("invalid drain proposal path".into()))?;
            let digits = version
                .strip_prefix('v')
                .ok_or_else(|| LeaseError::Invalid("invalid drain proposal version".into()))?;
            let hash = digest
                .strip_suffix(".json")
                .ok_or_else(|| LeaseError::Invalid("invalid drain proposal suffix".into()))?;
            if digits.len() != 20
                || !digits.bytes().all(|byte| byte.is_ascii_digit())
                || hash.len() != 64
                || !hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(LeaseError::Invalid(
                    "noncanonical drain proposal path".into(),
                ));
            }
            let version = digits
                .parse::<u64>()
                .map_err(|e| LeaseError::Invalid(e.to_string()))?;
            if version < before {
                candidates.push(object.location);
            }
            scanned += 1;
            if candidates.len() == 64 || scanned == 256 {
                break;
            }
        }
        drop(objects);
        // A later scheduled retention pass resumes this bounded batch. The admitted floor has
        // already excluded every candidate from checkpoint/assignment reuse.
        for candidate in candidates {
            match self.store.delete(&candidate).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(LeaseError::Io(error.to_string())),
            }
        }
        Ok(())
    }
}

fn snapshot_authority_error(error: SnapshotError) -> ClusterCheckpointAuthorityError {
    match error {
        SnapshotError::Io(reason) => LeaseError::Io(reason).into(),
        error => LeaseError::Invalid(error.to_string()).into(),
    }
}
