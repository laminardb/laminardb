//! Durable checkpoint-retention state transitions.

use super::{
    CheckpointDecisionStore, CheckpointRetentionCursor, CheckpointRetentionState,
    CheckpointRetentionUpdateResult, CheckpointScope, CommittedCheckpointRef, DecisionError,
    DecisionStoreUpdateMode, DurableCheckpointRetentionHead, ObjectStore, PutMode, PutOptions,
    PutPayload, UpdateVersion, VersionedCheckpointRetentionHead,
    CHECKPOINT_RETENTION_HEAD_MAX_BYTES, CHECKPOINT_RETENTION_HEAD_VERSION,
};

impl CheckpointDecisionStore {
    fn validate_retention_state_shape(
        state: &CheckpointRetentionState,
    ) -> Result<(), DecisionError> {
        let cursor = match state {
            CheckpointRetentionState::Idle { protected } => {
                protected.validate().map_err(DecisionError::Conflict)?;
                return Ok(());
            }
            CheckpointRetentionState::DeleteData { cursor }
            | CheckpointRetentionState::DeleteMetadata { cursor } => cursor,
        };
        cursor
            .protected
            .validate()
            .map_err(DecisionError::Conflict)?;
        cursor.current.validate().map_err(DecisionError::Conflict)?;
        if cursor.protected.epoch <= cursor.current.epoch {
            return Err(DecisionError::Conflict(
                "checkpoint retention cursor does not move behind its protected cut".into(),
            ));
        }
        if let Some(next) = &cursor.next {
            next.validate().map_err(DecisionError::Conflict)?;
            if next.epoch >= cursor.current.epoch {
                return Err(DecisionError::Conflict(
                    "checkpoint retention next cursor is not an older cut".into(),
                ));
            }
        }
        if let Some(stop_before) = &cursor.stop_before {
            stop_before.validate().map_err(DecisionError::Conflict)?;
            if stop_before.epoch >= cursor.current.epoch {
                return Err(DecisionError::Conflict(
                    "checkpoint retention boundary is not older than its current cut".into(),
                ));
            }
            let next = cursor.next.as_ref().ok_or_else(|| {
                DecisionError::Conflict(
                    "checkpoint retention crossed its exclusive lower boundary".into(),
                )
            })?;
            if next.epoch < stop_before.epoch
                || (next.epoch == stop_before.epoch && next != stop_before)
            {
                return Err(DecisionError::Conflict(
                    "checkpoint retention next cursor crossed its exclusive lower boundary".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_retention_head_shape(
        head: &DurableCheckpointRetentionHead,
        deployment_id: &str,
    ) -> Result<(), DecisionError> {
        if head.version != CHECKPOINT_RETENTION_HEAD_VERSION || head.deployment_id != deployment_id
        {
            return Err(DecisionError::Conflict(
                "checkpoint retention head has a foreign deployment or unsupported version".into(),
            ));
        }
        Self::validate_retention_state_shape(&head.state)
    }

    async fn read_retention_head(
        &self,
        deployment_id: &str,
    ) -> Result<Option<VersionedCheckpointRetentionHead>, DecisionError> {
        let Some(result) = self
            .get_control_record(
                &Self::retention_head_path(deployment_id),
                "checkpoint retention head",
                CHECKPOINT_RETENTION_HEAD_MAX_BYTES,
            )
            .await?
        else {
            return Ok(None);
        };
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        self.require_native_cas_token("checkpoint retention head", &update_version)?;
        let bytes = Self::read_control_record_bytes(
            result,
            "checkpoint retention head",
            CHECKPOINT_RETENTION_HEAD_MAX_BYTES,
            None,
        )
        .await?;
        let head: DurableCheckpointRetentionHead =
            serde_json::from_slice(&bytes).map_err(|error| {
                DecisionError::Conflict(format!("checkpoint retention head: {error}"))
            })?;
        Self::validate_retention_head_shape(&head, deployment_id)?;
        let canonical = serde_json::to_vec(&head)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(
                "checkpoint retention head does not use its canonical body".into(),
            ));
        }
        Ok(Some(VersionedCheckpointRetentionHead {
            head,
            update_version,
        }))
    }

    async fn put_retention_head(
        &self,
        deployment_id: &str,
        observed: Option<VersionedCheckpointRetentionHead>,
        state: CheckpointRetentionState,
    ) -> Result<CheckpointRetentionUpdateResult, DecisionError> {
        Self::validate_retention_state_shape(&state)?;
        let candidate = DurableCheckpointRetentionHead {
            version: CHECKPOINT_RETENTION_HEAD_VERSION,
            deployment_id: deployment_id.to_owned(),
            state: state.clone(),
        };
        let mode = match (self.update_mode, observed.as_ref()) {
            (_, None) => PutMode::Create,
            (DecisionStoreUpdateMode::NativeCas, Some(current)) => {
                PutMode::Update(current.update_version.clone())
            }
            (DecisionStoreUpdateMode::LocalSingleWriter, Some(_)) => PutMode::Overwrite,
        };
        let payload = Self::encode_control_record(
            "checkpoint retention head",
            &candidate,
            CHECKPOINT_RETENTION_HEAD_MAX_BYTES,
        )?;
        let result = self
            .store
            .put_opts(
                &Self::retention_head_path(deployment_id),
                PutPayload::from(payload),
                PutOptions {
                    mode,
                    ..PutOptions::default()
                },
            )
            .await
            .map(|_| ());
        if result.is_ok() {
            return Ok(CheckpointRetentionUpdateResult::Applied(state));
        }

        let winner = self.read_retention_head(deployment_id).await?;
        if winner
            .as_ref()
            .is_some_and(|winner| winner.head == candidate)
        {
            return Ok(CheckpointRetentionUpdateResult::Unchanged(state));
        }
        let changed = winner.as_ref().map(|winner| &winner.head)
            != observed.as_ref().map(|current| &current.head);
        let error = result.expect_err("failed retention-head write has an error");
        if changed
            || matches!(
                error,
                object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. }
                    | object_store::Error::NotFound { .. }
            )
        {
            return Ok(CheckpointRetentionUpdateResult::Conflict {
                current: winner.map(|winner| winner.head.state),
            });
        }
        Err(DecisionError::Io(error.to_string()))
    }

    async fn begin_checkpoint_retention_inner(
        &self,
        protected: &CommittedCheckpointRef,
    ) -> Result<CheckpointRetentionUpdateResult, DecisionError> {
        protected.validate().map_err(DecisionError::Conflict)?;
        let deployment_id = self.load_or_create_deployment_id().await?;
        let decision = self
            .read_decision_head(&deployment_id)
            .await?
            .and_then(|head| head.head.latest_commit)
            .and_then(|outcome| outcome.committed_checkpoint)
            .ok_or_else(|| {
                DecisionError::Conflict(
                    "checkpoint retention requires an authoritative local Commit".into(),
                )
            })?;
        if decision != *protected {
            return Err(DecisionError::Conflict(
                "checkpoint retention protected cut is not the authoritative latest Commit".into(),
            ));
        }

        let observed = self.read_retention_head(&deployment_id).await?;
        if let Some(current) = observed.as_ref() {
            match &current.head.state {
                CheckpointRetentionState::Idle {
                    protected: retained,
                } if retained == protected => {
                    return Ok(CheckpointRetentionUpdateResult::Unchanged(
                        current.head.state.clone(),
                    ));
                }
                CheckpointRetentionState::DeleteData { .. }
                | CheckpointRetentionState::DeleteMetadata { .. } => {
                    return Ok(CheckpointRetentionUpdateResult::Unchanged(
                        current.head.state.clone(),
                    ));
                }
                CheckpointRetentionState::Idle { .. } => {}
            }
        }

        let protected_index = self.load_committed_checkpoint(protected).await?;
        if protected_index.scope != CheckpointScope::Local {
            return Err(DecisionError::Conflict(
                "local checkpoint retention cannot protect a cluster checkpoint".into(),
            ));
        }
        let stop_before = match observed.as_ref().map(|current| &current.head.state) {
            Some(CheckpointRetentionState::Idle {
                protected: retained,
            }) => {
                if retained.epoch >= protected.epoch {
                    return Err(DecisionError::Conflict(
                        "checkpoint retention cannot replace its retained cut with an older cut"
                            .into(),
                    ));
                }
                let retained_index = self.load_committed_checkpoint(retained).await?;
                retained_index.predecessor
            }
            None => None,
            Some(
                CheckpointRetentionState::DeleteData { .. }
                | CheckpointRetentionState::DeleteMetadata { .. },
            ) => unreachable!("active retention returned above"),
        };
        let state = match protected_index.predecessor.as_ref() {
            None if observed.is_none() => CheckpointRetentionState::Idle {
                protected: protected.clone(),
            },
            None => {
                return Err(DecisionError::Conflict(
                    "authoritative Commit does not extend the retained checkpoint".into(),
                ));
            }
            Some(current) => {
                if let Some(CheckpointRetentionState::Idle {
                    protected: retained,
                }) = observed.as_ref().map(|head| &head.head.state)
                {
                    if current.epoch < retained.epoch
                        || (current.epoch == retained.epoch && current != retained)
                    {
                        return Err(DecisionError::Conflict(
                            "authoritative Commit does not extend the retained checkpoint".into(),
                        ));
                    }
                }
                let current_index = self.load_committed_checkpoint(current).await?;
                protected_index
                    .validate_predecessor_index(&current_index)
                    .map_err(DecisionError::Conflict)?;
                CheckpointRetentionState::DeleteData {
                    cursor: CheckpointRetentionCursor {
                        protected: protected.clone(),
                        current: current.clone(),
                        next: current_index.predecessor,
                        stop_before,
                    },
                }
            }
        };
        self.put_retention_head(&deployment_id, observed, state)
            .await
    }

    /// Start or resume retention for the authoritative latest local Commit.
    ///
    /// # Errors
    /// The protected cut is stale, its chain is invalid, or durable metadata cannot be updated.
    pub async fn begin_checkpoint_retention(
        &self,
        protected: &CommittedCheckpointRef,
    ) -> Result<CheckpointRetentionUpdateResult, DecisionError> {
        if self.update_mode == DecisionStoreUpdateMode::LocalSingleWriter {
            let lock = self.local_metadata_rmw_lock.as_ref().ok_or_else(|| {
                DecisionError::Conflict(
                    "local decision store is missing its namespace write lock".into(),
                )
            })?;
            let _guard = lock.lock().await;
            self.begin_checkpoint_retention_inner(protected).await
        } else {
            self.begin_checkpoint_retention_inner(protected).await
        }
    }

    async fn advance_checkpoint_retention_inner(
        &self,
        expected: &CheckpointRetentionState,
    ) -> Result<CheckpointRetentionUpdateResult, DecisionError> {
        Self::validate_retention_state_shape(expected)?;
        let deployment_id = self.load_or_create_deployment_id().await?;
        let observed = self.read_retention_head(&deployment_id).await?;
        if observed.as_ref().map(|head| &head.head.state) != Some(expected) {
            return Ok(CheckpointRetentionUpdateResult::Conflict {
                current: observed.map(|head| head.head.state),
            });
        }
        let state = match expected {
            CheckpointRetentionState::Idle { .. } => {
                return Err(DecisionError::Conflict(
                    "idle checkpoint retention has no destructive phase to advance".into(),
                ));
            }
            CheckpointRetentionState::DeleteData { cursor } => {
                CheckpointRetentionState::DeleteMetadata {
                    cursor: cursor.clone(),
                }
            }
            CheckpointRetentionState::DeleteMetadata { cursor }
                if cursor.next == cursor.stop_before =>
            {
                CheckpointRetentionState::Idle {
                    protected: cursor.protected.clone(),
                }
            }
            CheckpointRetentionState::DeleteMetadata { cursor } => {
                let current = cursor.next.as_ref().ok_or_else(|| {
                    DecisionError::Conflict(
                        "checkpoint retention ended before its exclusive lower boundary".into(),
                    )
                })?;
                let current_index = self.load_committed_checkpoint(current).await?;
                CheckpointRetentionState::DeleteData {
                    cursor: CheckpointRetentionCursor {
                        protected: cursor.protected.clone(),
                        current: current.clone(),
                        next: current_index.predecessor,
                        stop_before: cursor.stop_before.clone(),
                    },
                }
            }
        };
        self.put_retention_head(&deployment_id, observed, state)
            .await
    }

    /// Advance one completed retention phase with an exact compare-and-swap.
    ///
    /// # Errors
    /// The expected state is idle or invalid, or durable metadata cannot be updated.
    pub async fn advance_checkpoint_retention(
        &self,
        expected: &CheckpointRetentionState,
    ) -> Result<CheckpointRetentionUpdateResult, DecisionError> {
        if self.update_mode == DecisionStoreUpdateMode::LocalSingleWriter {
            let lock = self.local_metadata_rmw_lock.as_ref().ok_or_else(|| {
                DecisionError::Conflict(
                    "local decision store is missing its namespace write lock".into(),
                )
            })?;
            let _guard = lock.lock().await;
            self.advance_checkpoint_retention_inner(expected).await
        } else {
            self.advance_checkpoint_retention_inner(expected).await
        }
    }

    /// Read the exact local retention state without listing storage.
    ///
    /// # Errors
    /// Object-store I/O or malformed/foreign metadata.
    pub async fn checkpoint_retention_state(
        &self,
    ) -> Result<Option<CheckpointRetentionState>, DecisionError> {
        let deployment_id = self.load_or_create_deployment_id().await?;
        Ok(self
            .read_retention_head(&deployment_id)
            .await?
            .map(|head| head.head.state))
    }
}
