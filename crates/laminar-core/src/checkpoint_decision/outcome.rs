//! Terminal outcome and artifact-inventory publication.

use super::{
    CheckpointArtifactInventory, CheckpointArtifactInventoryUpdateResult,
    CheckpointAssignmentFence, CheckpointAttempt, CheckpointDecisionHead, CheckpointDecisionStore,
    CheckpointOutcome, CheckpointScope, CheckpointVerdict, CommittedCheckpointIndex,
    CommittedCheckpointRef, DecisionError, DecisionHeadCasResult, DecisionStoreUpdateMode,
    DurableCheckpointDecisionHead, LeaderProof, ObjectStore, PutMode, PutOptions, PutPayload,
    RecordOutcomeResult, UpdateVersion, VersionedCheckpointDecisionHead,
    CHECKPOINT_DECISION_HEAD_MAX_BYTES, CHECKPOINT_DECISION_HEAD_VERSION,
    CHECKPOINT_OUTCOME_VERSION,
};

impl CheckpointDecisionStore {
    pub(super) fn validate_decision_head_shape(
        head: &DurableCheckpointDecisionHead,
        deployment_id: &str,
    ) -> Result<(), DecisionError> {
        if head.version != CHECKPOINT_DECISION_HEAD_VERSION || head.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(
                "checkpoint decision head has a foreign deployment or unsupported version".into(),
            ));
        }
        if head.latest_terminal.is_none() && head.active_artifacts.is_none() {
            return Err(DecisionError::Conflict(
                "checkpoint decision head has neither a terminal outcome nor active artifacts"
                    .into(),
            ));
        }
        if let Some(terminal) = head.latest_terminal.as_ref() {
            terminal.validate_shape(terminal.epoch)?;
            if terminal.scope != CheckpointScope::Local || terminal.deployment_id != deployment_id {
                return Err(DecisionError::Conflict(
                    "checkpoint decision head contains a non-local terminal outcome".into(),
                ));
            }
        }
        match head.latest_commit.as_ref() {
            Some(commit) => {
                commit.validate_shape(commit.epoch)?;
                let terminal = head.latest_terminal.as_ref().ok_or_else(|| {
                    DecisionError::Conflict(
                        "checkpoint decision head has a Commit but no terminal outcome".into(),
                    )
                })?;
                if commit.scope != CheckpointScope::Local
                    || commit.deployment_id != deployment_id
                    || !commit.is_commit()
                    || commit.epoch > terminal.epoch
                    || (!terminal.is_commit() && commit.epoch == terminal.epoch)
                {
                    return Err(DecisionError::Conflict(
                        "checkpoint decision head contains an invalid latest Commit".into(),
                    ));
                }
                if terminal.is_commit() && commit != terminal {
                    return Err(DecisionError::Conflict(
                        "terminal Commit does not match the decision head's latest Commit".into(),
                    ));
                }
            }
            None if head
                .latest_terminal
                .as_ref()
                .is_some_and(CheckpointOutcome::is_commit) =>
            {
                return Err(DecisionError::Conflict(
                    "checkpoint decision head lost its terminal Commit".into(),
                ));
            }
            None => {}
        }
        if let Some(inventory) = head.active_artifacts.as_ref() {
            inventory.validate().map_err(|error| {
                DecisionError::Conflict(format!(
                    "checkpoint decision head contains invalid active artifacts: {error}"
                ))
            })?;
            if inventory.deployment_id != deployment_id || inventory.assignment_fence.is_some() {
                return Err(DecisionError::Conflict(
                    "local checkpoint decision head contains foreign or cluster artifacts".into(),
                ));
            }
            if let Some(terminal) = head.latest_terminal.as_ref() {
                if inventory.attempt.epoch < terminal.epoch
                    || (inventory.attempt.epoch == terminal.epoch
                        && (terminal.is_commit()
                            || inventory.attempt.checkpoint_id != terminal.checkpoint_id))
                {
                    return Err(DecisionError::Conflict(
                        "active checkpoint artifacts conflict with the latest terminal outcome"
                            .into(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) async fn read_decision_head(
        &self,
        deployment_id: &str,
    ) -> Result<Option<VersionedCheckpointDecisionHead>, DecisionError> {
        let path = Self::decision_head_path(deployment_id);
        let Some(result) = self
            .get_control_record(
                &path,
                "checkpoint decision head",
                CHECKPOINT_DECISION_HEAD_MAX_BYTES,
            )
            .await?
        else {
            return Ok(None);
        };
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        self.require_native_cas_token("checkpoint decision head", &update_version)?;
        let bytes = Self::read_control_record_bytes(
            result,
            "checkpoint decision head",
            CHECKPOINT_DECISION_HEAD_MAX_BYTES,
            None,
        )
        .await?;
        let head: DurableCheckpointDecisionHead =
            serde_json::from_slice(&bytes).map_err(|error| {
                DecisionError::Conflict(format!("checkpoint decision head: {error}"))
            })?;
        Self::validate_decision_head_shape(&head, deployment_id)?;
        let canonical = serde_json::to_vec(&head)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(
                "checkpoint decision head does not use its canonical body".into(),
            ));
        }
        Ok(Some(VersionedCheckpointDecisionHead {
            head,
            update_version,
        }))
    }

    async fn put_decision_head(
        &self,
        observed: Option<VersionedCheckpointDecisionHead>,
        candidate: DurableCheckpointDecisionHead,
    ) -> Result<DecisionHeadCasResult, DecisionError> {
        Self::validate_decision_head_shape(&candidate, &candidate.deployment_id)?;
        let mode = match (self.update_mode, observed.as_ref()) {
            (_, None) => PutMode::Create,
            (DecisionStoreUpdateMode::NativeCas, Some(current)) => {
                PutMode::Update(current.update_version.clone())
            }
            (DecisionStoreUpdateMode::LocalSingleWriter, Some(_)) => PutMode::Overwrite,
        };
        let payload = Self::encode_control_record(
            "checkpoint decision head",
            &candidate,
            CHECKPOINT_DECISION_HEAD_MAX_BYTES,
        )?;
        let result = self
            .store
            .put_opts(
                &Self::decision_head_path(&candidate.deployment_id),
                PutPayload::from(payload),
                PutOptions {
                    mode,
                    ..PutOptions::default()
                },
            )
            .await
            .map(|_| ());
        if result.is_ok() {
            return Ok(DecisionHeadCasResult::Applied);
        }

        let winner = self.read_decision_head(&candidate.deployment_id).await?;
        if winner
            .as_ref()
            .is_some_and(|winner| winner.head == candidate)
        {
            return Ok(DecisionHeadCasResult::Unchanged);
        }
        let changed = winner.as_ref().map(|winner| &winner.head)
            != observed.as_ref().map(|current| &current.head);
        let error = result.expect_err("failed decision-head write has an error");
        if changed
            || matches!(
                error,
                object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. }
                    | object_store::Error::NotFound { .. }
            )
        {
            return Ok(DecisionHeadCasResult::Conflict(
                winner.map(|winner| Box::new(winner.head)),
            ));
        }
        Err(DecisionError::Io(error.to_string()))
    }

    fn terminal_aborts_inventory(
        head: &DurableCheckpointDecisionHead,
        inventory: &CheckpointArtifactInventory,
    ) -> bool {
        head.latest_terminal.as_ref().is_some_and(|terminal| {
            !terminal.is_commit()
                && terminal.epoch == inventory.attempt.epoch
                && terminal.checkpoint_id == inventory.attempt.checkpoint_id
        })
    }

    async fn begin_checkpoint_artifact_inventory_inner(
        &self,
        inventory: CheckpointArtifactInventory,
    ) -> Result<CheckpointArtifactInventoryUpdateResult, DecisionError> {
        inventory.validate().map_err(DecisionError::Conflict)?;
        if inventory.assignment_fence.is_some() {
            return Err(DecisionError::Conflict(
                "local checkpoint artifact inventory cannot carry an assignment fence".into(),
            ));
        }
        let deployment_id = self.load_or_create_deployment_id().await?;
        if inventory.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(
                "checkpoint artifact inventory belongs to a foreign deployment".into(),
            ));
        }
        let observed = self.read_decision_head(&deployment_id).await?;
        if let Some(active) = observed
            .as_ref()
            .and_then(|current| current.head.active_artifacts.as_ref())
        {
            return Ok(
                if active == &inventory
                    && !observed.as_ref().is_some_and(|current| {
                        Self::terminal_aborts_inventory(&current.head, &inventory)
                    })
                {
                    CheckpointArtifactInventoryUpdateResult::Unchanged
                } else {
                    CheckpointArtifactInventoryUpdateResult::Conflict {
                        current: Some(active.clone()),
                    }
                },
            );
        }
        if observed
            .as_ref()
            .and_then(|current| current.head.latest_terminal.as_ref())
            .is_some_and(|terminal| terminal.epoch >= inventory.attempt.epoch)
        {
            return Ok(CheckpointArtifactInventoryUpdateResult::Conflict { current: None });
        }

        let candidate = DurableCheckpointDecisionHead {
            version: CHECKPOINT_DECISION_HEAD_VERSION,
            deployment_id,
            latest_terminal: observed
                .as_ref()
                .and_then(|current| current.head.latest_terminal.clone()),
            latest_commit: observed
                .as_ref()
                .and_then(|current| current.head.latest_commit.clone()),
            active_artifacts: Some(inventory.clone()),
        };
        match self.put_decision_head(observed, candidate).await? {
            DecisionHeadCasResult::Applied => Ok(CheckpointArtifactInventoryUpdateResult::Applied),
            DecisionHeadCasResult::Unchanged => {
                Ok(CheckpointArtifactInventoryUpdateResult::Unchanged)
            }
            DecisionHeadCasResult::Conflict(current) => {
                let exact_unaborted = current.as_ref().is_some_and(|head| {
                    head.active_artifacts.as_ref() == Some(&inventory)
                        && !Self::terminal_aborts_inventory(head, &inventory)
                });
                let active = current.and_then(|head| head.active_artifacts);
                if exact_unaborted {
                    Ok(CheckpointArtifactInventoryUpdateResult::Unchanged)
                } else {
                    Ok(CheckpointArtifactInventoryUpdateResult::Conflict { current: active })
                }
            }
        }
    }

    /// Durably admit one exact local attempt before any checkpoint artifact is written.
    ///
    /// Equal retries converge. A different active attempt or a reused terminal attempt conflicts.
    ///
    /// # Errors
    /// Object-store I/O or a malformed, cluster, or foreign-deployment inventory.
    pub async fn begin_checkpoint_artifact_inventory(
        &self,
        inventory: CheckpointArtifactInventory,
    ) -> Result<CheckpointArtifactInventoryUpdateResult, DecisionError> {
        if self.update_mode == DecisionStoreUpdateMode::LocalSingleWriter {
            let lock = self.local_metadata_rmw_lock.as_ref().ok_or_else(|| {
                DecisionError::Conflict(
                    "local decision store is missing its namespace write lock".into(),
                )
            })?;
            let _guard = lock.lock().await;
            self.begin_checkpoint_artifact_inventory_inner(inventory)
                .await
        } else {
            self.begin_checkpoint_artifact_inventory_inner(inventory)
                .await
        }
    }

    async fn complete_checkpoint_artifact_cleanup_inner(
        &self,
        expected: &CheckpointArtifactInventory,
    ) -> Result<CheckpointArtifactInventoryUpdateResult, DecisionError> {
        expected.validate().map_err(DecisionError::Conflict)?;
        if expected.assignment_fence.is_some() {
            return Err(DecisionError::Conflict(
                "local checkpoint artifact inventory cannot carry an assignment fence".into(),
            ));
        }
        let deployment_id = self.load_or_create_deployment_id().await?;
        if expected.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(
                "checkpoint artifact inventory belongs to a foreign deployment".into(),
            ));
        }
        let observed = self.read_decision_head(&deployment_id).await?;
        let Some(current) = observed.as_ref() else {
            return Ok(CheckpointArtifactInventoryUpdateResult::Conflict { current: None });
        };
        match current.head.active_artifacts.as_ref() {
            Some(active) if active != expected => {
                return Ok(CheckpointArtifactInventoryUpdateResult::Conflict {
                    current: Some(active.clone()),
                });
            }
            None if Self::terminal_aborts_inventory(&current.head, expected) => {
                return Ok(CheckpointArtifactInventoryUpdateResult::Unchanged);
            }
            None => {
                return Ok(CheckpointArtifactInventoryUpdateResult::Conflict { current: None });
            }
            Some(_) => {}
        }
        if !Self::terminal_aborts_inventory(&current.head, expected) {
            return Ok(CheckpointArtifactInventoryUpdateResult::Conflict {
                current: Some(expected.clone()),
            });
        }

        let candidate = DurableCheckpointDecisionHead {
            version: CHECKPOINT_DECISION_HEAD_VERSION,
            deployment_id,
            latest_terminal: current.head.latest_terminal.clone(),
            latest_commit: current.head.latest_commit.clone(),
            active_artifacts: None,
        };
        match self.put_decision_head(observed, candidate).await? {
            DecisionHeadCasResult::Applied => Ok(CheckpointArtifactInventoryUpdateResult::Applied),
            DecisionHeadCasResult::Unchanged => {
                Ok(CheckpointArtifactInventoryUpdateResult::Unchanged)
            }
            DecisionHeadCasResult::Conflict(current) => {
                let active = current
                    .as_ref()
                    .and_then(|head| head.active_artifacts.clone());
                if active.is_none()
                    && current
                        .as_ref()
                        .is_some_and(|head| Self::terminal_aborts_inventory(head, expected))
                {
                    Ok(CheckpointArtifactInventoryUpdateResult::Unchanged)
                } else {
                    Ok(CheckpointArtifactInventoryUpdateResult::Conflict { current: active })
                }
            }
        }
    }

    /// Clear an exact local artifact inventory after its durable Abort paths are sealed.
    ///
    /// # Errors
    /// Object-store I/O or a malformed, cluster, or foreign-deployment inventory.
    pub async fn complete_checkpoint_artifact_cleanup(
        &self,
        expected: &CheckpointArtifactInventory,
    ) -> Result<CheckpointArtifactInventoryUpdateResult, DecisionError> {
        if self.update_mode == DecisionStoreUpdateMode::LocalSingleWriter {
            let lock = self.local_metadata_rmw_lock.as_ref().ok_or_else(|| {
                DecisionError::Conflict(
                    "local decision store is missing its namespace write lock".into(),
                )
            })?;
            let _guard = lock.lock().await;
            self.complete_checkpoint_artifact_cleanup_inner(expected)
                .await
        } else {
            self.complete_checkpoint_artifact_cleanup_inner(expected)
                .await
        }
    }

    pub(crate) async fn canonical_outcome_with_index(
        &self,
        epoch: u64,
        checkpoint_id: u64,
        scope: CheckpointScope,
        assignment_fence: Option<CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        verdict: CheckpointVerdict,
        committed_checkpoint: Option<CommittedCheckpointRef>,
    ) -> Result<(CheckpointOutcome, Option<CommittedCheckpointIndex>), DecisionError> {
        let outcome = CheckpointOutcome {
            version: CHECKPOINT_OUTCOME_VERSION,
            scope,
            epoch,
            checkpoint_id,
            deployment_id: self.load_or_create_deployment_id().await?,
            assignment_fence,
            leader_proof,
            committed_checkpoint,
            verdict,
        };
        outcome.validate_shape(epoch)?;
        let index = if outcome.is_commit() {
            Some(
                self.validate_committed_checkpoint_for_outcome(&outcome)
                    .await?,
            )
        } else {
            None
        };
        Ok((outcome, index))
    }

    async fn record_outcome_inner(
        &self,
        candidate: CheckpointOutcome,
        committed_index: Option<CommittedCheckpointIndex>,
    ) -> Result<RecordOutcomeResult, DecisionError> {
        let observed = self.read_decision_head(&candidate.deployment_id).await?;
        if let Some(terminal) = observed
            .as_ref()
            .and_then(|current| current.head.latest_terminal.as_ref())
        {
            if terminal.epoch == candidate.epoch {
                return if terminal == &candidate {
                    Ok(RecordOutcomeResult::Unchanged(candidate))
                } else {
                    Ok(RecordOutcomeResult::Conflict {
                        winner: terminal.clone(),
                    })
                };
            }
            if terminal.epoch > candidate.epoch {
                return Ok(RecordOutcomeResult::Conflict {
                    winner: terminal.clone(),
                });
            }
        }

        let active = observed
            .as_ref()
            .and_then(|current| current.head.active_artifacts.as_ref());
        let candidate_attempt = CheckpointAttempt::new(candidate.epoch, candidate.checkpoint_id);
        if active.is_some_and(|active| active.attempt != candidate_attempt) {
            return Err(DecisionError::Conflict(format!(
                "checkpoint outcome attempt {} does not match the active artifact inventory",
                candidate.checkpoint_id
            )));
        }

        if candidate.is_commit() {
            let active = active.ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "checkpoint Commit {} has no durable artifact inventory",
                    candidate.checkpoint_id
                ))
            })?;
            let index = committed_index.as_ref().ok_or_else(|| {
                DecisionError::Conflict("Commit has no validated committed checkpoint".into())
            })?;
            if index.pipeline_identity != active.pipeline_identity
                || index.assignment_fence != active.assignment_fence
            {
                return Err(DecisionError::Conflict(
                    "Commit metadata does not match the active checkpoint artifact inventory"
                        .into(),
                ));
            }
            let expected_predecessor = observed.as_ref().and_then(|current| {
                current
                    .head
                    .latest_commit
                    .as_ref()
                    .and_then(|commit| commit.committed_checkpoint.clone())
            });
            if index.predecessor != expected_predecessor {
                return Err(DecisionError::Conflict(format!(
                    "Commit epoch {} does not extend the authoritative committed checkpoint",
                    candidate.epoch
                )));
            }
            if let Some(predecessor_ref) = expected_predecessor.as_ref() {
                let predecessor = self.load_committed_checkpoint(predecessor_ref).await?;
                index
                    .validate_predecessor_index(&predecessor)
                    .map_err(DecisionError::Conflict)?;
            }
        }

        let head = DurableCheckpointDecisionHead {
            version: CHECKPOINT_DECISION_HEAD_VERSION,
            deployment_id: candidate.deployment_id.clone(),
            latest_commit: if candidate.is_commit() {
                Some(candidate.clone())
            } else {
                observed
                    .as_ref()
                    .and_then(|current| current.head.latest_commit.clone())
            },
            latest_terminal: Some(candidate.clone()),
            active_artifacts: if candidate.is_commit() {
                None
            } else {
                active.cloned()
            },
        };
        match self.put_decision_head(observed, head).await? {
            DecisionHeadCasResult::Applied => Ok(RecordOutcomeResult::Created(candidate)),
            DecisionHeadCasResult::Unchanged => Ok(RecordOutcomeResult::Unchanged(candidate)),
            DecisionHeadCasResult::Conflict(winner) => {
                if let Some(terminal) = winner.and_then(|head| head.latest_terminal) {
                    if terminal.epoch >= candidate.epoch {
                        return if terminal == candidate {
                            Ok(RecordOutcomeResult::Unchanged(candidate))
                        } else {
                            Ok(RecordOutcomeResult::Conflict { winner: terminal })
                        };
                    }
                }
                Err(DecisionError::Conflict(format!(
                    "checkpoint decision head contention did not publish epoch {}",
                    candidate.epoch
                )))
            }
        }
    }

    /// Publish the authoritative local terminal outcome.
    ///
    /// The singleton CAS is the decision: a crash before it leaves the attempt unresolved, while a
    /// crash after it leaves both the latest terminal and latest Commit directly recoverable.
    /// Equal retries converge; stale epochs and conflicting outcomes return the durable winner.
    ///
    /// # Errors
    /// Object-store I/O, malformed metadata, a forked Commit predecessor, or cluster authority.
    pub async fn record_outcome(
        &self,
        epoch: u64,
        checkpoint_id: u64,
        scope: CheckpointScope,
        assignment_fence: Option<CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        verdict: CheckpointVerdict,
        committed_checkpoint: Option<CommittedCheckpointRef>,
    ) -> Result<RecordOutcomeResult, DecisionError> {
        if scope == CheckpointScope::Cluster {
            return Err(DecisionError::Conflict(
                "cluster outcomes must be admitted through the shared leader authority".into(),
            ));
        }
        let (candidate, committed_index) = self
            .canonical_outcome_with_index(
                epoch,
                checkpoint_id,
                scope,
                assignment_fence,
                leader_proof,
                verdict,
                committed_checkpoint,
            )
            .await?;
        if self.update_mode == DecisionStoreUpdateMode::LocalSingleWriter {
            let lock = self.local_metadata_rmw_lock.as_ref().ok_or_else(|| {
                DecisionError::Conflict(
                    "local decision store is missing its namespace write lock".into(),
                )
            })?;
            let _guard = lock.lock().await;
            self.record_outcome_inner(candidate, committed_index).await
        } else {
            self.record_outcome_inner(candidate, committed_index).await
        }
    }

    /// Read the exact authoritative local decision head without listing storage.
    ///
    /// # Errors
    /// Object-store I/O or malformed/foreign head metadata.
    pub async fn checkpoint_decision_head(
        &self,
    ) -> Result<Option<CheckpointDecisionHead>, DecisionError> {
        let deployment_id = self.load_or_create_deployment_id().await?;
        Ok(self
            .read_decision_head(&deployment_id)
            .await?
            .map(|versioned| CheckpointDecisionHead {
                latest_terminal: versioned.head.latest_terminal,
                latest_commit: versioned.head.latest_commit,
                active_artifacts: versioned.head.active_artifacts,
            }))
    }

    /// Read the latest authoritative local terminal outcome without listing storage.
    ///
    /// # Errors
    /// Object-store I/O or malformed/foreign head metadata.
    pub async fn latest_terminal_outcome(
        &self,
    ) -> Result<Option<CheckpointOutcome>, DecisionError> {
        Ok(self
            .checkpoint_decision_head()
            .await?
            .and_then(|head| head.latest_terminal))
    }

    /// Read the latest authoritative local Commit without listing storage.
    ///
    /// # Errors
    /// Object-store I/O or malformed/foreign head metadata.
    pub async fn latest_committed_outcome(
        &self,
    ) -> Result<Option<CheckpointOutcome>, DecisionError> {
        Ok(self
            .checkpoint_decision_head()
            .await?
            .and_then(|head| head.latest_commit))
    }
}
