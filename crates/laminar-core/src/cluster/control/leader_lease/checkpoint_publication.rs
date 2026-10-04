use super::*;

impl LeaderLeaseStore {
    pub(super) async fn record_cluster_outcome_inner(
        &self,
        proof: &LeaderProof,
        epoch: u64,
        checkpoint_id: u64,
        assignment_fence: CheckpointAssignmentFence,
        verdict: CheckpointVerdict,
        committed_checkpoint: Option<CommittedCheckpointRef>,
        require_unadmitted: bool,
    ) -> Result<RecordOutcomeResult, ClusterCheckpointAuthorityError> {
        if !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        let attempt = crate::checkpoint::CheckpointAttempt::new(epoch, checkpoint_id);
        if !attempt.is_canonical() {
            return Err(DecisionError::Conflict(
                "cluster checkpoint outcomes require one nonzero canonical checkpoint ID".into(),
            )
            .into());
        }
        let initial = self
            .load_record()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        if !initial.lease.matches_proof(proof) {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        let decisions = CheckpointDecisionStore::new(Arc::clone(&self.store));
        let (candidate, committed_index) = decisions
            .canonical_outcome_with_index(
                epoch,
                checkpoint_id,
                CheckpointScope::Cluster,
                Some(assignment_fence),
                Some(proof.clone()),
                verdict,
                committed_checkpoint,
            )
            .await?;

        loop {
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &published.record;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            let snapshot = self.cached_audited_cluster_outcomes_from(current).await?;
            let outcomes = &snapshot.outcomes;
            if let Some((index, winner)) = outcomes
                .iter()
                .enumerate()
                .find(|(_, outcome)| outcome.epoch == candidate.epoch)
            {
                if require_unadmitted && winner == &candidate {
                    let link = snapshot.terminal_links.get(index).ok_or_else(|| {
                        LeaseError::Invalid("reserved Abort has no authority link".into())
                    })?;
                    let anchor = read_authority_record(self.store.as_ref(), link.sequence)
                        .await?
                        .ok_or_else(|| {
                            LeaseError::Invalid("reserved Abort anchor is missing".into())
                        })?;
                    if anchor.active_checkpoint_artifacts.is_some() {
                        return Err(DecisionError::Conflict(
                            "admitted checkpoint Abort cannot become reservation-only cleanup"
                                .into(),
                        )
                        .into());
                    }
                }
                return if winner == &candidate {
                    Ok(RecordOutcomeResult::Unchanged(winner.clone()))
                } else {
                    Ok(RecordOutcomeResult::Conflict {
                        winner: winner.clone(),
                    })
                };
            }
            if require_unadmitted && current.active_checkpoint_artifacts.is_some() {
                return Err(DecisionError::Conflict(
                    "checkpoint reservation already has admitted artifacts".into(),
                )
                .into());
            }
            let active = match current.active_checkpoint_artifacts.as_ref() {
                Some(active)
                    if active.deployment_id == candidate.deployment_id
                        && active.attempt.epoch == candidate.epoch
                        && active.attempt.checkpoint_id == candidate.checkpoint_id
                        && active.assignment_fence.as_ref()
                            == candidate.assignment_fence.as_ref() =>
                {
                    Some(active)
                }
                Some(_) => {
                    return Err(DecisionError::Conflict(format!(
                        "cluster checkpoint {} does not match the active artifact inventory",
                        candidate.checkpoint_id
                    ))
                    .into());
                }
                None if candidate.is_commit() => {
                    return Err(DecisionError::Conflict(format!(
                        "cluster Commit checkpoint {} has no admitted artifact inventory",
                        candidate.checkpoint_id
                    ))
                    .into());
                }
                None => None,
            };
            if candidate.is_commit()
                && current.active_checkpoint_artifact_leader_proof.as_ref() != Some(proof)
            {
                return Err(DecisionError::Conflict(format!(
                    "takeover leader cannot Commit checkpoint {} admitted by an older leader term",
                    candidate.checkpoint_id
                ))
                .into());
            }
            if let Some(last) = outcomes.last() {
                if candidate.checkpoint_id <= last.checkpoint_id {
                    return Err(DecisionError::Conflict(format!(
                        "cluster checkpoint {} does not advance durable checkpoint {}",
                        candidate.checkpoint_id, last.checkpoint_id
                    ))
                    .into());
                }
            }
            if candidate.is_commit()
                && current
                    .assignment_handoff_pin
                    .as_ref()
                    .is_some_and(|pin| candidate.assignment_fence.as_ref() != Some(&pin.target))
            {
                return Err(DecisionError::Conflict(
                    "cluster Commit does not bind the active assignment handoff target".into(),
                )
                .into());
            }
            let (commit_index, expected_predecessor) = Self::validate_commit_index_for_append(
                &candidate,
                committed_index.as_ref(),
                outcomes,
                active,
            )?;
            if candidate.is_commit() && snapshot.commit_links.len() >= MAX_LIVE_AUTHORITY_LINKS {
                return Err(DecisionError::Conflict(format!(
                    "live Commit retention reached the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound; advance the artifact-retention horizon before admitting another Commit"
                ))
                .into());
            }
            current.validate_outcome_retention_floor(&candidate)?;
            if Box::pin(
                self.compact_cluster_outcome_history_before_append(proof, current, &snapshot),
            )
            .await?
            {
                tokio::task::yield_now().await;
                continue;
            }
            if let (Some(index), Some(predecessor_ref)) =
                (commit_index, expected_predecessor.as_ref())
            {
                let predecessor = decisions.load_committed_checkpoint(predecessor_ref).await?;
                self.validate_checkpoint_predecessor_from(current, index, &predecessor)
                    .await?;
            }

            let base_sequence = current.lease.seq;
            let sequence = base_sequence
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("leader authority sequence exhausted".into()))?;
            let mut next = current.preserve_with_lease(LeaderLease {
                seq: sequence,
                renewal_sequence: current.lease.renewal_sequence,
                token: current.lease.token,
                owner: current.lease.owner.clone(),
                expires_at_ms: current.lease.expires_at_ms,
                catalog_manifest: current.lease.catalog_manifest.clone(),
            });
            next.checkpoint_outcome = Some(candidate.clone());
            next.previous_outcome = current.outcome_head;
            let new_link = OutcomeLink {
                sequence,
                epoch: candidate.epoch,
                checkpoint_id: candidate.checkpoint_id,
            };
            next.outcome_head = Some(new_link);
            next.record_topology_cut_outcome(&candidate)?;
            if candidate.is_commit() {
                next.previous_commit = current.commit_head;
                next.commit_head = Some(new_link);
                next.active_checkpoint_artifacts = None;
                next.active_checkpoint_artifact_leader_proof = None;
                if next
                    .assignment_handoff_pin
                    .as_ref()
                    .is_some_and(|pin| candidate.assignment_fence.as_ref() == Some(&pin.target))
                {
                    next.assignment_handoff_pin = None;
                }
            }
            next.validate()?;
            match self
                .create_authority_record(Some(&published), &next)
                .await?
            {
                AuthorityCreateOutcome::Created => {
                    let mut appended = outcomes.to_vec();
                    appended.push(candidate.clone());
                    let mut terminal_links = snapshot.terminal_links.to_vec();
                    terminal_links.push(new_link);
                    let mut commit_links = snapshot.commit_links.to_vec();
                    if candidate.is_commit() {
                        commit_links.push(new_link);
                    }
                    self.install_cluster_outcome_audit(
                        Self::cluster_outcome_audit_key(&next),
                        next.lease.seq,
                        ClusterOutcomeAuditSnapshot {
                            outcomes: Arc::from(appended),
                            terminal_links: Arc::from(terminal_links),
                            commit_links: Arc::from(commit_links),
                        },
                    );
                    return Ok(RecordOutcomeResult::Created(candidate));
                }
                AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(RecordOutcomeResult::Unchanged(candidate));
                }
                AuthorityCreateOutcome::Contended(winner_head) => {
                    let winners = self
                        .cached_audited_cluster_outcomes_from(&winner_head)
                        .await?;
                    if let Some(winner) = winners
                        .outcomes
                        .iter()
                        .find(|outcome| outcome.epoch == candidate.epoch)
                    {
                        return if winner == &candidate {
                            Ok(RecordOutcomeResult::Unchanged(winner.clone()))
                        } else {
                            Ok(RecordOutcomeResult::Conflict {
                                winner: winner.clone(),
                            })
                        };
                    }
                    if !winner_head.lease.matches_proof(proof) {
                        return Err(ClusterCheckpointAuthorityError::Fenced);
                    }
                    if winner_head.lease.seq <= base_sequence {
                        return Err(LeaseError::Invalid(
                            "cluster outcome contention did not advance the authority sequence"
                                .into(),
                        )
                        .into());
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }
}

impl LeaderLeaseStore {
    fn validate_commit_index_for_append<'a>(
        candidate: &CheckpointOutcome,
        committed_index: Option<&'a CommittedCheckpointIndex>,
        outcomes: &[CheckpointOutcome],
        active: Option<&CheckpointArtifactInventory>,
    ) -> Result<
        (
            Option<&'a CommittedCheckpointIndex>,
            Option<CommittedCheckpointRef>,
        ),
        DecisionError,
    > {
        Ok(if candidate.is_commit() {
            let index = committed_index.ok_or_else(|| {
                DecisionError::Conflict(
                    "canonical cluster Commit is missing its committed checkpoint index".into(),
                )
            })?;
            let expected_predecessor = outcomes
                .iter()
                .rev()
                .find(|outcome| outcome.is_commit())
                .and_then(|outcome| outcome.committed_checkpoint.clone());
            if index.predecessor != expected_predecessor {
                return Err(DecisionError::Conflict(format!(
                    "cluster Commit checkpoint {} does not extend the authoritative Commit head",
                    candidate.checkpoint_id
                )));
            }
            if active.is_none_or(|active| index.pipeline_identity != active.pipeline_identity) {
                return Err(DecisionError::Conflict(format!(
                    "cluster Commit checkpoint {} does not match its admitted pipeline identity",
                    candidate.checkpoint_id
                )));
            }
            (Some(index), expected_predecessor)
        } else {
            (None, None)
        })
    }
}

impl LeaderAuthorityRecord {
    fn validate_outcome_retention_floor(
        &self,
        candidate: &CheckpointOutcome,
    ) -> Result<(), DecisionError> {
        if let Some(floor) = self.outcome_floor.as_ref() {
            if candidate.deployment_id != floor.deployment_id
                || candidate.epoch < floor.authority_before_epoch
            {
                return Err(DecisionError::Conflict(format!(
                    "cluster outcome epoch {} is below or outside authority floor {}",
                    candidate.epoch, floor.authority_before_epoch
                )));
            }
        }
        Ok(())
    }
}
