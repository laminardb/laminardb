//! Immutable committed-checkpoint index creation, validation, and deletion.

use super::{
    AbortedCommittedCheckpointSeal, Bytes, CheckpointDecisionStore, CheckpointOutcome,
    CommittedCheckpointIndex, CommittedCheckpointRef, DecisionError, ObjectStore, ObjectStoreExt,
    PutMode, PutOptions, PutPayload, ABORTED_COMMITTED_CHECKPOINT_SEAL_VERSION,
    MAX_COMMITTED_CHECKPOINT_INDEX_BYTES,
};

impl CheckpointDecisionStore {
    async fn load_committed_checkpoint_bytes(
        &self,
        reference: &CommittedCheckpointRef,
        expected_len: Option<u64>,
    ) -> Result<Option<Bytes>, DecisionError> {
        reference.validate().map_err(DecisionError::Conflict)?;
        let record = format!("committed checkpoint '{}'", reference.sha256);
        let Some(result) = self
            .get_control_record(
                &Self::committed_checkpoint_path(reference),
                &record,
                u64::try_from(MAX_COMMITTED_CHECKPOINT_INDEX_BYTES).unwrap_or(u64::MAX),
            )
            .await?
        else {
            return Ok(None);
        };
        Self::read_control_record_bytes(
            result,
            &record,
            u64::try_from(MAX_COMMITTED_CHECKPOINT_INDEX_BYTES).unwrap_or(u64::MAX),
            expected_len,
        )
        .await
        .map(Some)
    }

    /// Create the canonical content-addressed body for a committed checkpoint index.
    ///
    /// Identical retries converge on the existing immutable body. The returned reference is safe
    /// to publish only after this method succeeds.
    ///
    /// # Errors
    /// Object-store I/O, malformed index content, deployment mismatch, or a conflicting body.
    pub async fn create_committed_checkpoint(
        &self,
        index: &CommittedCheckpointIndex,
    ) -> Result<CommittedCheckpointRef, DecisionError> {
        let (encoded, reference) = index
            .encode_and_reference()
            .map_err(DecisionError::Conflict)?;
        let deployment_id = self.load_or_create_deployment_id().await?;
        if index.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(format!(
                "committed checkpoint belongs to deployment {}, current deployment is {deployment_id}",
                index.deployment_id
            )));
        }

        let path = Self::committed_checkpoint_path(&reference);
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(Bytes::from(encoded)), options)
            .await
        {
            Ok(_) => Ok(reference),
            Err(put_error) => match self.load_committed_checkpoint(&reference).await {
                Ok(stored) if stored == *index => Ok(reference),
                Ok(_) => Err(DecisionError::Conflict(format!(
                    "committed checkpoint '{}' differs from the proposed content",
                    reference.sha256
                ))),
                Err(reconcile_error) => Err(DecisionError::Io(format!(
                    "committed checkpoint write failed ({put_error}); reconciliation failed ({reconcile_error})"
                ))),
            },
        }
    }

    /// Load and verify one exact content-addressed committed checkpoint index.
    ///
    /// Verification covers the object path, recorded and observed lengths, canonical JSON body,
    /// SHA-256 reference, deployment identity, and committed-index invariants.
    ///
    /// # Errors
    /// Object-store I/O, a missing object, malformed content, or any reference mismatch.
    pub async fn load_committed_checkpoint(
        &self,
        reference: &CommittedCheckpointRef,
    ) -> Result<CommittedCheckpointIndex, DecisionError> {
        self.load_committed_checkpoint_optional(reference)
            .await?
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "committed checkpoint '{}' is missing",
                    reference.sha256
                ))
            })
    }

    async fn load_committed_checkpoint_optional(
        &self,
        reference: &CommittedCheckpointRef,
    ) -> Result<Option<CommittedCheckpointIndex>, DecisionError> {
        let Some(bytes) = self
            .load_committed_checkpoint_bytes(reference, Some(reference.len))
            .await?
        else {
            return Ok(None);
        };
        let index: CommittedCheckpointIndex = serde_json::from_slice(&bytes).map_err(|error| {
            DecisionError::Conflict(format!(
                "committed checkpoint '{}': {error}",
                reference.sha256
            ))
        })?;
        let (canonical, actual_reference) = index
            .encode_and_reference()
            .map_err(DecisionError::Conflict)?;
        if actual_reference != *reference || canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(format!(
                "committed checkpoint '{}' does not match its content-addressed reference",
                reference.sha256
            )));
        }
        let deployment_id = self.load_or_create_deployment_id().await?;
        if index.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(format!(
                "committed checkpoint belongs to deployment {}, current deployment is {deployment_id}",
                index.deployment_id
            )));
        }
        Ok(Some(index))
    }

    /// Permanently seal the exact content-addressed candidate for an aborted attempt.
    ///
    /// The seal occupies the candidate's existing path, so an in-flight conditional create can
    /// either win and be replaced or lose to the seal. Identical retries converge.
    ///
    /// # Errors
    /// The candidate is malformed or foreign, the path contains different content, or object-store
    /// I/O cannot be reconciled to the exact seal.
    pub async fn seal_aborted_committed_checkpoint_candidate(
        &self,
        index: &CommittedCheckpointIndex,
    ) -> Result<(), DecisionError> {
        let (candidate_bytes, reference) = index
            .encode_and_reference()
            .map_err(DecisionError::Conflict)?;
        let deployment_id = self.load_or_create_deployment_id().await?;
        if index.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(format!(
                "committed checkpoint belongs to deployment {}, current deployment is {deployment_id}",
                index.deployment_id
            )));
        }
        let seal_bytes = crate::checkpoint::canonical_json_bytes(&AbortedCommittedCheckpointSeal {
            version: ABORTED_COMMITTED_CHECKPOINT_SEAL_VERSION,
            deployment_id: &deployment_id,
            candidate: &reference,
        })
        .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let path = Self::committed_checkpoint_path(&reference);

        let observed = self
            .load_committed_checkpoint_bytes(&reference, None)
            .await?;
        let mode = match observed.as_deref() {
            None => PutMode::Create,
            Some(bytes) if bytes == seal_bytes.as_slice() => return Ok(()),
            Some(bytes) if bytes == candidate_bytes.as_slice() => PutMode::Overwrite,
            Some(_) => {
                return Err(DecisionError::Conflict(format!(
                    "committed checkpoint '{}' contains neither the exact candidate nor its abort seal",
                    reference.sha256
                )));
            }
        };
        let Err(mut write_error) = self
            .store
            .put_opts(
                &path,
                PutPayload::from(Bytes::from(seal_bytes.clone())),
                PutOptions {
                    mode: mode.clone(),
                    ..PutOptions::default()
                },
            )
            .await
        else {
            return Ok(());
        };

        let mut reconciled = self
            .load_committed_checkpoint_bytes(&reference, None)
            .await?;
        if reconciled.as_deref() == Some(seal_bytes.as_slice()) {
            return Ok(());
        }
        if matches!(mode, PutMode::Create)
            && reconciled.as_deref() == Some(candidate_bytes.as_slice())
        {
            let Err(error) = self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(Bytes::from(seal_bytes.clone())),
                    PutOptions {
                        mode: PutMode::Overwrite,
                        ..PutOptions::default()
                    },
                )
                .await
            else {
                return Ok(());
            };
            write_error = error;
            reconciled = self
                .load_committed_checkpoint_bytes(&reference, None)
                .await?;
            if reconciled.as_deref() == Some(seal_bytes.as_slice()) {
                return Ok(());
            }
        }

        if reconciled
            .as_deref()
            .is_some_and(|bytes| bytes != candidate_bytes.as_slice())
        {
            return Err(DecisionError::Conflict(format!(
                "committed checkpoint '{}' changed to content other than its exact abort seal",
                reference.sha256
            )));
        }
        Err(DecisionError::Io(format!(
            "committed checkpoint '{}' abort seal write failed and did not become durable: {write_error}",
            reference.sha256
        )))
    }

    /// Delete one exact committed index after validating any extant body.
    ///
    /// Missing objects and ambiguous deletes that removed the object are successful retries.
    ///
    /// # Errors
    /// The reference, extant index, deployment identity, or object-store operation is invalid.
    pub async fn delete_committed_checkpoint(
        &self,
        reference: &CommittedCheckpointRef,
    ) -> Result<(), DecisionError> {
        reference.validate().map_err(DecisionError::Conflict)?;
        if self
            .load_committed_checkpoint_optional(reference)
            .await?
            .is_none()
        {
            return Ok(());
        }
        let path = Self::committed_checkpoint_path(reference);
        match self.store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(delete_error) => match self.store.head(&path).await {
                Err(object_store::Error::NotFound { .. }) => Ok(()),
                Ok(_) => Err(DecisionError::Io(delete_error.to_string())),
                Err(reconcile_error) => Err(DecisionError::Io(format!(
                    "committed checkpoint delete failed ({delete_error}); reconciliation failed ({reconcile_error})"
                ))),
            },
        }
    }

    pub(crate) async fn validate_committed_checkpoint_for_outcome(
        &self,
        outcome: &CheckpointOutcome,
    ) -> Result<CommittedCheckpointIndex, DecisionError> {
        outcome.validate_shape(outcome.epoch)?;
        let reference = outcome.committed_checkpoint.as_ref().ok_or_else(|| {
            DecisionError::Conflict(format!(
                "commit outcome for epoch {} requires a committed checkpoint",
                outcome.epoch
            ))
        })?;
        let index = self.load_committed_checkpoint(reference).await?;
        if index.epoch != outcome.epoch
            || index.checkpoint_id != outcome.checkpoint_id
            || index.scope != outcome.scope
            || index.assignment_fence.as_ref() != outcome.assignment_fence.as_ref()
            || index.deployment_id != outcome.deployment_id
        {
            return Err(DecisionError::Conflict(format!(
                "commit outcome for epoch {} does not match committed checkpoint '{}'",
                outcome.epoch, reference.sha256
            )));
        }
        Ok(index)
    }
}
