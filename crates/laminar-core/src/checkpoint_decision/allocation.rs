//! Monotonic checkpoint ID reservation and allocation.

use super::{
    Bytes, CheckpointDecisionStore, DecisionError, DecisionStoreUpdateMode, DeploymentIdentity,
    ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion, VersionedCheckpointIdHead,
    DEPLOYMENT_IDENTITY_MAX_BYTES, DEPLOYMENT_IDENTITY_VERSION, LOCAL_RESERVATION_SIZE,
};

impl CheckpointDecisionStore {
    fn consume_local_reservation(&self, minimum: u64) -> Option<u64> {
        let mut reservation = self.local_reservation.lock();
        let checkpoint_id = reservation.next_id?.max(minimum);
        if checkpoint_id > reservation.end {
            reservation.next_id = None;
            return None;
        }
        reservation.next_id = checkpoint_id
            .checked_add(1)
            .filter(|next| *next <= reservation.end);
        Some(checkpoint_id)
    }

    fn install_local_reservation(&self, first: u64, end: u64) {
        let mut reservation = self.local_reservation.lock();
        reservation.end = end;
        reservation.next_id = first.checked_add(1).filter(|next| *next <= end);
    }

    async fn allocate_local_checkpoint_id_at_least(
        &self,
        deployment_id: &str,
        minimum: u64,
    ) -> Result<u64, DecisionError> {
        if let Some(checkpoint_id) = self.consume_local_reservation(minimum) {
            return Ok(checkpoint_id);
        }
        let lock = self.local_metadata_rmw_lock.as_ref().ok_or_else(|| {
            DecisionError::Conflict(
                "local decision store is missing its namespace write lock".into(),
            )
        })?;
        let _guard = lock.lock().await;
        let current = self
            .read_checkpoint_id_head(deployment_id)
            .await?
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "checkpoint ID authority for deployment {deployment_id} disappeared"
                ))
            })?;
        let checkpoint_id = current
            .head
            .checkpoint_id
            .checked_add(1)
            .map(|next| next.max(minimum))
            .ok_or_else(|| DecisionError::Conflict("checkpoint ID space exhausted u64".into()))?;
        let reservation_end = checkpoint_id.saturating_add(LOCAL_RESERVATION_SIZE - 1);
        let head = DeploymentIdentity {
            version: DEPLOYMENT_IDENTITY_VERSION,
            id: deployment_id.to_owned(),
            allocator_mode: self.update_mode,
            checkpoint_id: reservation_end,
            allocation_id: uuid::Uuid::now_v7().to_string(),
        };
        let payload = Self::encode_control_record(
            "deployment identity",
            &head,
            DEPLOYMENT_IDENTITY_MAX_BYTES,
        )?;
        let result = self
            .store
            .put_opts(
                &Self::deployment_identity_path(),
                PutPayload::from(payload),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..PutOptions::default()
                },
            )
            .await;
        match result {
            Ok(put_result) => {
                self.cache_checkpoint_id_head(Some(VersionedCheckpointIdHead {
                    head,
                    update_version: put_result.into(),
                }));
                self.install_local_reservation(checkpoint_id, reservation_end);
                Ok(checkpoint_id)
            }
            Err(error) => {
                let observed = self.read_checkpoint_id_head(deployment_id).await?;
                if observed.as_ref().is_some_and(|value| value.head == head) {
                    self.cache_checkpoint_id_head(observed);
                    self.install_local_reservation(checkpoint_id, reservation_end);
                    Ok(checkpoint_id)
                } else {
                    self.cache_checkpoint_id_head(observed);
                    Err(DecisionError::Io(error.to_string()))
                }
            }
        }
    }

    async fn read_checkpoint_id_head(
        &self,
        deployment_id: &str,
    ) -> Result<Option<VersionedCheckpointIdHead>, DecisionError> {
        let observed = self.read_deployment_identity().await?;
        if let Some(observed) = observed.as_ref() {
            if observed.head.id != deployment_id {
                return Err(DecisionError::Conflict(format!(
                    "checkpoint ID head belongs to deployment {}, current deployment is {deployment_id}",
                    observed.head.id
                )));
            }
        }
        Ok(observed)
    }

    pub(super) fn cache_checkpoint_id_head(&self, head: Option<VersionedCheckpointIdHead>) {
        *self.checkpoint_id_head.lock() = head;
    }

    fn validate_checkpoint_id_head_progress(
        prior: Option<&VersionedCheckpointIdHead>,
        observed: Option<&VersionedCheckpointIdHead>,
    ) -> Result<(), DecisionError> {
        match (prior, observed) {
            (Some(prior), None) => Err(DecisionError::Conflict(format!(
                "checkpoint ID head disappeared after durable ID {}",
                prior.head.checkpoint_id
            ))),
            (Some(prior), Some(observed))
                if observed.head.checkpoint_id < prior.head.checkpoint_id =>
            {
                Err(DecisionError::Conflict(format!(
                    "checkpoint ID head regressed from {} to {}",
                    prior.head.checkpoint_id, observed.head.checkpoint_id
                )))
            }
            (Some(prior), Some(observed))
                if observed.head.checkpoint_id == prior.head.checkpoint_id
                    && observed.head != prior.head =>
            {
                Err(DecisionError::Conflict(format!(
                    "checkpoint ID {} changed allocation identity without advancing",
                    prior.head.checkpoint_id
                )))
            }
            _ => Ok(()),
        }
    }

    async fn allocate_shared_checkpoint_id_at_least(
        &self,
        deployment_id: &str,
        minimum: u64,
    ) -> Result<u64, DecisionError> {
        let allocation_id = uuid::Uuid::now_v7().to_string();
        let mut current = self.checkpoint_id_head.lock().clone();
        if current.is_none() {
            current = self.read_checkpoint_id_head(deployment_id).await?;
            if current.is_none() {
                return Err(DecisionError::Conflict(format!(
                    "checkpoint ID authority for deployment {deployment_id} disappeared"
                )));
            }
            self.cache_checkpoint_id_head(current.clone());
        }

        loop {
            let versioned = current.as_ref().ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "checkpoint ID authority for deployment {deployment_id} disappeared"
                ))
            })?;
            let checkpoint_id = versioned
                .head
                .checkpoint_id
                .checked_add(1)
                .map(|next| next.max(minimum))
                .ok_or_else(|| {
                    DecisionError::Conflict("checkpoint ID space exhausted u64".to_owned())
                })?;
            let head = DeploymentIdentity {
                version: DEPLOYMENT_IDENTITY_VERSION,
                id: deployment_id.to_owned(),
                allocator_mode: self.update_mode,
                checkpoint_id,
                allocation_id: allocation_id.clone(),
            };
            let payload = serde_json::to_vec(&head)
                .map(Bytes::from)
                .map_err(|error| DecisionError::Conflict(error.to_string()))?;
            let mode = PutMode::Update(versioned.update_version.clone());
            let result = self
                .store
                .put_opts(
                    &Self::deployment_identity_path(),
                    PutPayload::from(payload),
                    PutOptions {
                        mode,
                        ..PutOptions::default()
                    },
                )
                .await;
            match result {
                Ok(put_result) => {
                    let update_version: UpdateVersion = put_result.into();
                    if update_version.e_tag.is_none() && update_version.version.is_none() {
                        // The create/update itself is authoritative, but this response cannot
                        // safely seed the next CAS. Force a metadata read on the next allocation.
                        self.cache_checkpoint_id_head(None);
                    } else {
                        self.cache_checkpoint_id_head(Some(VersionedCheckpointIdHead {
                            head,
                            update_version,
                        }));
                    }
                    return Ok(checkpoint_id);
                }
                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. }
                    | object_store::Error::NotFound { .. },
                ) => {
                    let observed = self.read_checkpoint_id_head(deployment_id).await?;
                    Self::validate_checkpoint_id_head_progress(
                        current.as_ref(),
                        observed.as_ref(),
                    )?;
                    current = observed;
                    self.cache_checkpoint_id_head(current.clone());
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    let observed = self.read_checkpoint_id_head(deployment_id).await?;
                    Self::validate_checkpoint_id_head_progress(
                        current.as_ref(),
                        observed.as_ref(),
                    )?;
                    if observed.as_ref().is_some_and(|value| value.head == head) {
                        self.cache_checkpoint_id_head(observed);
                        return Ok(checkpoint_id);
                    }
                    if observed
                        .as_ref()
                        .is_some_and(|value| value.head.checkpoint_id >= checkpoint_id)
                    {
                        current = observed;
                        self.cache_checkpoint_id_head(current.clone());
                        tokio::task::yield_now().await;
                        continue;
                    }
                    self.cache_checkpoint_id_head(observed);
                    return Err(DecisionError::Io(error.to_string()));
                }
            }
        }
    }

    /// Allocate the next globally ordered checkpoint ID at or above `minimum`.
    ///
    /// Shared stores advance exactly one ID with native compare-and-swap. A certified local single
    /// writer reserves a durable range in the deployment singleton, consumes it in memory, and
    /// burns any unused suffix after restart.
    ///
    /// # Errors
    /// Object-store I/O, malformed or foreign durable state, a shared store without conditional
    /// update support, or exhaustion of the `u64` ID space.
    pub async fn allocate_checkpoint_id_at_least(
        &self,
        minimum: u64,
    ) -> Result<u64, DecisionError> {
        if minimum == 0 {
            return Err(DecisionError::Conflict(
                "minimum checkpoint ID must be nonzero".to_owned(),
            ));
        }

        let deployment_id = self.load_or_create_deployment_id().await?;
        let _guard = self.metadata_write_lock.lock().await;
        match self.update_mode {
            DecisionStoreUpdateMode::NativeCas => {
                self.allocate_shared_checkpoint_id_at_least(&deployment_id, minimum)
                    .await
            }
            DecisionStoreUpdateMode::LocalSingleWriter => {
                self.allocate_local_checkpoint_id_at_least(&deployment_id, minimum)
                    .await
            }
        }
    }

    #[cfg(test)]
    pub(super) async fn checkpoint_id_reservation_high_watermark(
        &self,
    ) -> Result<u64, DecisionError> {
        let deployment_id = self.load_or_create_deployment_id().await?;
        self.read_checkpoint_id_head(&deployment_id)
            .await?
            .map(|head| head.head.checkpoint_id)
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "checkpoint ID authority for deployment {deployment_id} disappeared"
                ))
            })
    }

    #[cfg(test)]
    /// Allocate from the default floor in unit tests.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable allocator is unavailable or invalid.
    pub async fn allocate_checkpoint_id(&self) -> Result<u64, DecisionError> {
        self.allocate_checkpoint_id_at_least(1).await
    }
}
