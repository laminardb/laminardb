//! Durable deployment identity creation and validation.

use super::{
    CheckpointDecisionStore, DecisionError, DecisionStoreUpdateMode, DeploymentIdentity,
    ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion, VersionedCheckpointIdHead,
    DEPLOYMENT_IDENTITY_MAX_BYTES, DEPLOYMENT_IDENTITY_VERSION,
};

impl CheckpointDecisionStore {
    fn validate_deployment_identity(
        identity: &DeploymentIdentity,
    ) -> Result<String, DecisionError> {
        if identity.version != DEPLOYMENT_IDENTITY_VERSION {
            return Err(DecisionError::Conflict(format!(
                "deployment identity version {} is unsupported (expected \
                 {DEPLOYMENT_IDENTITY_VERSION})",
                identity.version
            )));
        }
        let parsed = uuid::Uuid::parse_str(&identity.id).map_err(|error| {
            DecisionError::Conflict(format!("deployment identity is not a UUID: {error}"))
        })?;
        let canonical = parsed.to_string();
        if canonical != identity.id || parsed.is_nil() {
            return Err(DecisionError::Conflict(
                "deployment identity must be a canonical non-nil UUID".into(),
            ));
        }
        let allocation_id = uuid::Uuid::parse_str(&identity.allocation_id).map_err(|error| {
            DecisionError::Conflict(format!(
                "deployment identity has invalid allocation identity: {error}"
            ))
        })?;
        if allocation_id.is_nil() || allocation_id.to_string() != identity.allocation_id {
            return Err(DecisionError::Conflict(
                "deployment identity must use a canonical non-nil allocation identity".into(),
            ));
        }
        Ok(canonical)
    }

    pub(super) async fn read_deployment_identity(
        &self,
    ) -> Result<Option<VersionedCheckpointIdHead>, DecisionError> {
        let Some(result) = self
            .get_control_record(
                &Self::deployment_identity_path(),
                "deployment identity",
                DEPLOYMENT_IDENTITY_MAX_BYTES,
            )
            .await?
        else {
            return Ok(None);
        };
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        self.require_native_cas_token("deployment identity", &update_version)?;
        let bytes = Self::read_control_record_bytes(
            result,
            "deployment identity",
            DEPLOYMENT_IDENTITY_MAX_BYTES,
            None,
        )
        .await?;
        let identity: DeploymentIdentity = serde_json::from_slice(&bytes)
            .map_err(|error| DecisionError::Conflict(format!("deployment identity: {error}")))?;
        Self::validate_deployment_identity(&identity)?;
        if identity.allocator_mode != self.update_mode {
            return Err(DecisionError::Conflict(format!(
                "deployment identity allocator mode {:?} cannot be opened as {:?}",
                identity.allocator_mode, self.update_mode
            )));
        }
        let canonical = serde_json::to_vec(&identity)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(
                "deployment identity does not use its canonical body".into(),
            ));
        }
        Ok(Some(VersionedCheckpointIdHead {
            head: identity,
            update_version,
        }))
    }

    /// Load the checkpoint namespace's create-once deployment incarnation, creating it when the
    /// durable store is empty. Concurrent cluster members converge through object-store CAS.
    ///
    /// # Errors
    /// Object-store I/O or a malformed/conflicting persisted identity.
    pub async fn load_or_create_deployment_id(&self) -> Result<String, DecisionError> {
        if let Some(identity) = self.deployment_id.get() {
            return Ok(identity.clone());
        }
        let _guard = self.metadata_write_lock.lock().await;
        if let Some(identity) = self.deployment_id.get() {
            return Ok(identity.clone());
        }
        if let Some(stored) = self.read_deployment_identity().await? {
            let identity = stored.head.id.clone();
            self.cache_checkpoint_id_head(Some(stored));
            let _ = self.deployment_id.set(identity.clone());
            return Ok(identity);
        }

        let identity = DeploymentIdentity {
            version: DEPLOYMENT_IDENTITY_VERSION,
            id: uuid::Uuid::now_v7().to_string(),
            allocator_mode: self.update_mode,
            checkpoint_id: 0,
            allocation_id: uuid::Uuid::now_v7().to_string(),
        };
        let payload = Self::encode_control_record(
            "deployment identity",
            &identity,
            DEPLOYMENT_IDENTITY_MAX_BYTES,
        )?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(
                &Self::deployment_identity_path(),
                PutPayload::from(payload),
                options,
            )
            .await
        {
            Ok(put_result) => {
                let update_version: UpdateVersion = put_result.into();
                if self.update_mode == DecisionStoreUpdateMode::NativeCas
                    && update_version.e_tag.is_none()
                    && update_version.version.is_none()
                {
                    self.cache_checkpoint_id_head(None);
                } else {
                    self.cache_checkpoint_id_head(Some(VersionedCheckpointIdHead {
                        head: identity.clone(),
                        update_version,
                    }));
                }
                let _ = self.deployment_id.set(identity.id.clone());
                Ok(identity.id)
            }
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. },
            ) => {
                let stored = self.read_deployment_identity().await?.ok_or_else(|| {
                    DecisionError::Conflict(
                        "deployment identity disappeared after create conflict".into(),
                    )
                })?;
                let identity = stored.head.id.clone();
                self.cache_checkpoint_id_head(Some(stored));
                let _ = self.deployment_id.set(identity.clone());
                Ok(identity)
            }
            Err(error) => match self.read_deployment_identity().await? {
                Some(stored) => {
                    let identity = stored.head.id.clone();
                    self.cache_checkpoint_id_head(Some(stored));
                    let _ = self.deployment_id.set(identity.clone());
                    Ok(identity)
                }
                None => Err(DecisionError::Io(error.to_string())),
            },
        }
    }
}
