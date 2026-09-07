//! Audited checkpoint settlement and commit-authority observation.

use crate::checkpoint::{CheckpointAssignmentFence, CheckpointAttempt, LeaderProof};
use crate::checkpoint_decision::{CheckpointOutcome, DecisionError};

use super::{ClusterCheckpointAuthorityError, LeaderLeaseStore};

/// Durable settlement state for one exact cluster checkpoint attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterAttemptStatus {
    /// No terminal outcome exists and the admitting leader term can still publish one.
    Pending,
    /// An exact or newer terminal outcome durably settles the attempt.
    Settled(Box<CheckpointOutcome>),
    /// No terminal outcome exists, but the leader term that admitted the artifacts was superseded.
    ///
    /// Takeover and outcome publication serialize through the same authority sequence. Once this
    /// state is observed, the old term cannot publish a Commit; coordinated recovery still owns
    /// publication of the authoritative Abort and artifact cleanup.
    CommitFenced,
}

fn settlement_from_outcomes(
    attempt: CheckpointAttempt,
    outcomes: &[CheckpointOutcome],
) -> Option<CheckpointOutcome> {
    if let Ok(index) = outcomes.binary_search_by_key(&attempt.epoch, |outcome| outcome.epoch) {
        return Some(outcomes[index].clone());
    }
    outcomes
        .last()
        .filter(|highest| highest.checkpoint_id > attempt.checkpoint_id)
        .cloned()
}

impl LeaderLeaseStore {
    /// Return the exact immutable outcome for `attempt`, or the first audited terminal outcome
    /// known to close that older checkpoint. Compacted continuity anchors are included in the
    /// audit.
    ///
    /// # Errors
    /// Returns an error for a noncanonical attempt identity or an unavailable or invalid durable
    /// authority chain.
    pub async fn cluster_attempt_settlement(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        if !attempt.is_canonical() {
            return Err(DecisionError::Conflict(
                "cluster checkpoint settlement requires one nonzero canonical checkpoint ID".into(),
            )
            .into());
        }
        let outcomes = self.audited_cluster_outcomes().await?.1;
        Ok(settlement_from_outcomes(attempt, &outcomes))
    }

    /// Audit the settlement and commit authority of one admitted cluster checkpoint attempt.
    ///
    /// Unlike [`Self::cluster_attempt_settlement`], this distinguishes a live pending attempt from
    /// an attempt whose admitting leader term has been durably superseded. The distinction is
    /// emitted only when the same audited authority head both retains the exact artifact inventory
    /// and proves that its admitting leader proof no longer owns the lease.
    ///
    /// # Errors
    /// Returns an error for malformed identities, an unavailable or invalid authority chain, or an
    /// unsettled attempt that does not exactly match the retained artifact inventory.
    pub async fn cluster_attempt_status(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: &CheckpointAssignmentFence,
        leader_proof: &LeaderProof,
    ) -> Result<ClusterAttemptStatus, ClusterCheckpointAuthorityError> {
        if !attempt.is_canonical() {
            return Err(DecisionError::Conflict(
                "cluster checkpoint status requires one nonzero canonical checkpoint ID".into(),
            )
            .into());
        }
        if !assignment_fence.is_canonical() {
            return Err(DecisionError::Conflict(
                "cluster checkpoint status requires a canonical assignment fence".into(),
            )
            .into());
        }
        if !leader_proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        if assignment_fence.participant_incarnation(leader_proof.owner.node_id)
            != Some(leader_proof.owner.boot_id)
        {
            return Err(DecisionError::Conflict(
                "cluster checkpoint status leader is outside the assignment fence".into(),
            )
            .into());
        }

        let (head, outcomes) = self.audited_cluster_outcomes().await?;
        if let Some(settlement) = settlement_from_outcomes(attempt, &outcomes) {
            return Ok(ClusterAttemptStatus::Settled(Box::new(settlement)));
        }
        let head = head.ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        let active = head.active_checkpoint_artifacts.as_ref().ok_or_else(|| {
            DecisionError::Conflict(format!(
                "unsettled cluster checkpoint {} has no active artifact inventory",
                attempt.checkpoint_id
            ))
        })?;
        if active.attempt != attempt
            || active.assignment_fence.as_ref() != Some(assignment_fence)
            || head.active_checkpoint_artifact_leader_proof.as_ref() != Some(leader_proof)
        {
            return Err(DecisionError::Conflict(format!(
                "unsettled cluster checkpoint {} does not match its admitted artifact authority",
                attempt.checkpoint_id
            ))
            .into());
        }
        if head.lease.matches_proof(leader_proof) {
            Ok(ClusterAttemptStatus::Pending)
        } else {
            Ok(ClusterAttemptStatus::CommitFenced)
        }
    }
}
