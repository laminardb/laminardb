//! Create-once sink-open witness transitions.

use super::{
    Bytes, CheckpointAttempt, CheckpointDecisionStore, CheckpointSinkOpenWitness,
    CheckpointSinkOpenWitnessSlot, CheckpointSinkOpenWitnessSlotState, DecisionError,
    DecisionStoreUpdateMode, ObjectStore, PipelineIdentity, PutMode, PutOptions, PutPayload,
    UpdateVersion, VersionedCheckpointSinkOpenWitnessSlot, CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES,
    CHECKPOINT_SINK_OPEN_WITNESS_MAX_SINKS, CHECKPOINT_SINK_OPEN_WITNESS_SLOT_VERSION,
    CHECKPOINT_SINK_OPEN_WITNESS_VERSION,
};

impl CheckpointDecisionStore {
    fn validate_sink_open_witness_shape(
        witness: &CheckpointSinkOpenWitness,
    ) -> Result<(), DecisionError> {
        if witness.version != CHECKPOINT_SINK_OPEN_WITNESS_VERSION {
            return Err(DecisionError::Conflict(format!(
                "sink-open witness version {} is unsupported (expected \
                 {CHECKPOINT_SINK_OPEN_WITNESS_VERSION})",
                witness.version
            )));
        }
        let deployment = uuid::Uuid::parse_str(&witness.deployment_id).map_err(|error| {
            DecisionError::Conflict(format!(
                "sink-open witness has invalid deployment identity: {error}"
            ))
        })?;
        if deployment.is_nil() || deployment.to_string() != witness.deployment_id {
            return Err(DecisionError::Conflict(
                "sink-open witness must use a canonical non-nil deployment identity".into(),
            ));
        }
        if let Some(error) = witness.pipeline_identity.validation_error() {
            return Err(DecisionError::Conflict(format!(
                "sink-open witness has an invalid pipeline identity: {error}"
            )));
        }
        if !witness.attempt.is_canonical() {
            return Err(DecisionError::Conflict(
                "sink-open witness must use one nonzero canonical checkpoint ID".into(),
            ));
        }
        if witness.committable_sinks.is_empty()
            || witness.committable_sinks.len() > CHECKPOINT_SINK_OPEN_WITNESS_MAX_SINKS
        {
            return Err(DecisionError::Conflict(format!(
                "sink-open witness must name between 1 and \
                 {CHECKPOINT_SINK_OPEN_WITNESS_MAX_SINKS} committable sinks"
            )));
        }
        if witness
            .committable_sinks
            .iter()
            .any(|name| name.is_empty() || name.trim() != name)
        {
            return Err(DecisionError::Conflict(
                "sink-open witness contains a non-canonical sink name".into(),
            ));
        }
        if witness
            .committable_sinks
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(DecisionError::Conflict(
                "sink-open witness sink names must be strictly sorted and unique".into(),
            ));
        }
        let create_token = uuid::Uuid::parse_str(&witness.create_token).map_err(|error| {
            DecisionError::Conflict(format!(
                "sink-open witness has invalid create token: {error}"
            ))
        })?;
        if create_token.is_nil() || create_token.to_string() != witness.create_token {
            return Err(DecisionError::Conflict(
                "sink-open witness must use a canonical non-nil create token".into(),
            ));
        }
        Ok(())
    }

    fn validate_sink_open_witness_slot_shape(
        slot: &CheckpointSinkOpenWitnessSlot,
    ) -> Result<(), DecisionError> {
        if slot.version != CHECKPOINT_SINK_OPEN_WITNESS_SLOT_VERSION {
            return Err(DecisionError::Conflict(format!(
                "sink-open witness slot version {} is unsupported (expected \
                 {CHECKPOINT_SINK_OPEN_WITNESS_SLOT_VERSION})",
                slot.version
            )));
        }
        Self::validate_sink_open_witness_shape(slot.witness())?;
        if let CheckpointSinkOpenWitnessSlotState::Closed { close_token, .. } = &slot.state {
            let token = uuid::Uuid::parse_str(close_token).map_err(|error| {
                DecisionError::Conflict(format!(
                    "sink-open witness slot has invalid close token: {error}"
                ))
            })?;
            if token.is_nil() || token.to_string() != *close_token {
                return Err(DecisionError::Conflict(
                    "sink-open witness slot must use a canonical non-nil close token".into(),
                ));
            }
        }
        Ok(())
    }

    fn encode_sink_open_witness_slot(
        slot: &CheckpointSinkOpenWitnessSlot,
    ) -> Result<Bytes, DecisionError> {
        Self::validate_sink_open_witness_slot_shape(slot)?;
        Self::encode_control_record(
            "sink-open witness slot",
            slot,
            CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES,
        )
    }

    async fn read_sink_open_witness_record(
        &self,
    ) -> Result<Option<VersionedCheckpointSinkOpenWitnessSlot>, DecisionError> {
        let Some(result) = self
            .get_control_record(
                &Self::sink_open_witness_path(),
                "sink-open witness",
                CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES,
            )
            .await?
        else {
            return Ok(None);
        };
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        self.require_native_cas_token("sink-open witness slot", &update_version)?;
        let bytes = Self::read_control_record_bytes(
            result,
            "sink-open witness",
            CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES,
            None,
        )
        .await?;
        let slot: CheckpointSinkOpenWitnessSlot = serde_json::from_slice(&bytes)
            .map_err(|error| DecisionError::Conflict(format!("sink-open witness slot: {error}")))?;
        Self::validate_sink_open_witness_slot_shape(&slot)?;
        let canonical = serde_json::to_vec(&slot)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(format!(
                "sink-open witness slot for checkpoint {} does not use its canonical body",
                slot.witness().attempt.checkpoint_id
            )));
        }
        Ok(Some(VersionedCheckpointSinkOpenWitnessSlot {
            slot,
            update_version,
        }))
    }

    fn validate_sink_open_witness_slot_deployment(
        slot: &CheckpointSinkOpenWitnessSlot,
        deployment_id: &str,
    ) -> Result<(), DecisionError> {
        if slot.witness().deployment_id == deployment_id {
            return Ok(());
        }
        Err(DecisionError::Conflict(format!(
            "sink-open witness belongs to deployment {}, current deployment is {deployment_id}",
            slot.witness().deployment_id
        )))
    }

    fn sink_open_witness_put_mode(&self, expected: Option<UpdateVersion>) -> PutMode {
        match (self.update_mode, expected) {
            (_, None) => PutMode::Create,
            (DecisionStoreUpdateMode::NativeCas, Some(version)) => PutMode::Update(version),
            (DecisionStoreUpdateMode::LocalSingleWriter, Some(_)) => PutMode::Overwrite,
        }
    }

    async fn put_sink_open_witness_slot(
        &self,
        slot: &CheckpointSinkOpenWitnessSlot,
        expected: Option<UpdateVersion>,
    ) -> Result<(), object_store::Error> {
        let payload = Self::encode_sink_open_witness_slot(slot).map_err(|error| {
            object_store::Error::Generic {
                store: "CheckpointDecisionStore",
                source: Box::new(error),
            }
        })?;
        self.store
            .put_opts(
                &Self::sink_open_witness_path(),
                PutPayload::from(payload),
                PutOptions {
                    mode: self.sink_open_witness_put_mode(expected),
                    ..PutOptions::default()
                },
            )
            .await
            .map(|_| ())
    }

    /// Read the singleton sink-open owner record.
    ///
    /// # Errors
    /// Object-store I/O, malformed/non-canonical metadata, or foreign deployment state.
    pub async fn sink_open_witness(
        &self,
    ) -> Result<Option<CheckpointSinkOpenWitness>, DecisionError> {
        let deployment_id = self.load_or_create_deployment_id().await?;
        let Some(versioned) = self.read_sink_open_witness_record().await? else {
            return Ok(None);
        };
        Self::validate_sink_open_witness_slot_deployment(&versioned.slot, &deployment_id)?;
        match versioned.slot.state {
            CheckpointSinkOpenWitnessSlotState::Open { witness } => Ok(Some(witness)),
            CheckpointSinkOpenWitnessSlotState::Closed { .. } => Ok(None),
        }
    }

    /// Create the durable witness before invoking any checkpoint-committable sink begin call.
    ///
    /// `committable_sinks` must already be strictly sorted and unique so duplicate runtime names
    /// cannot be silently collapsed into one recovery participant.
    ///
    /// # Errors
    /// Object-store I/O, invalid input, or any malformed, foreign, or conflicting live witness.
    pub async fn create_sink_open_witness(
        &self,
        pipeline_identity: PipelineIdentity,
        participant_id: u64,
        attempt: CheckpointAttempt,
        committable_sinks: Vec<String>,
    ) -> Result<CheckpointSinkOpenWitness, DecisionError> {
        let candidate = CheckpointSinkOpenWitness {
            version: CHECKPOINT_SINK_OPEN_WITNESS_VERSION,
            deployment_id: self.load_or_create_deployment_id().await?,
            pipeline_identity,
            participant_id,
            attempt,
            committable_sinks,
            create_token: uuid::Uuid::now_v7().to_string(),
        };
        Self::validate_sink_open_witness_shape(&candidate)?;
        Self::encode_sink_open_witness_slot(&CheckpointSinkOpenWitnessSlot::open(
            candidate.clone(),
        ))?;
        // An accepted open must always have enough room for its mandatory close tombstone.
        Self::encode_sink_open_witness_slot(&CheckpointSinkOpenWitnessSlot::closed(
            candidate.clone(),
        ))?;

        match self.update_mode {
            DecisionStoreUpdateMode::NativeCas => {
                self.create_sink_open_witness_inner(candidate).await
            }
            DecisionStoreUpdateMode::LocalSingleWriter => {
                let local_lock = self.local_metadata_rmw_lock.as_ref().ok_or_else(|| {
                    DecisionError::Conflict(
                        "local decision store is missing its namespace write lock".to_owned(),
                    )
                })?;
                let _guard = local_lock.lock().await;
                self.create_sink_open_witness_inner(candidate).await
            }
        }
    }

    async fn create_sink_open_witness_inner(
        &self,
        candidate: CheckpointSinkOpenWitness,
    ) -> Result<CheckpointSinkOpenWitness, DecisionError> {
        let current = self.read_sink_open_witness_record().await?;
        let (expected, prior_slot) = match current {
            None => (None, None),
            Some(versioned) => {
                Self::validate_sink_open_witness_slot_deployment(
                    &versioned.slot,
                    &candidate.deployment_id,
                )?;
                match &versioned.slot.state {
                    CheckpointSinkOpenWitnessSlotState::Open { witness } => {
                        return Err(DecisionError::Conflict(format!(
                            "sink-open witness create for checkpoint {} observed conflicting \
                             checkpoint {}",
                            candidate.attempt.checkpoint_id, witness.attempt.checkpoint_id
                        )));
                    }
                    CheckpointSinkOpenWitnessSlotState::Closed { witness, .. }
                        if candidate.attempt.checkpoint_id <= witness.attempt.checkpoint_id =>
                    {
                        return Err(DecisionError::Conflict(format!(
                            "sink-open witness checkpoint {} does not advance closed checkpoint {}",
                            candidate.attempt.checkpoint_id, witness.attempt.checkpoint_id
                        )));
                    }
                    CheckpointSinkOpenWitnessSlotState::Closed { .. } => {}
                }
                (Some(versioned.update_version), Some(versioned.slot))
            }
        };
        let candidate_slot = CheckpointSinkOpenWitnessSlot::open(candidate.clone());
        let create_error = match self
            .put_sink_open_witness_slot(&candidate_slot, expected)
            .await
        {
            Ok(()) => return Ok(candidate),
            Err(error) => error,
        };

        // Only this proposal's exact create token proves that an ambiguous open transition won.
        if let Some(observed) = self.read_sink_open_witness_record().await? {
            Self::validate_sink_open_witness_slot_deployment(
                &observed.slot,
                &candidate.deployment_id,
            )?;
            if observed.slot == candidate_slot {
                return Ok(candidate);
            }
            let conditional_conflict = matches!(
                &create_error,
                object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. }
                    | object_store::Error::NotFound { .. }
            );
            if !conditional_conflict
                && prior_slot
                    .as_ref()
                    .is_some_and(|prior| prior == &observed.slot)
            {
                return Err(DecisionError::Io(create_error.to_string()));
            }
            return Err(DecisionError::Conflict(format!(
                "sink-open witness create for checkpoint {} observed conflicting checkpoint {}",
                candidate.attempt.checkpoint_id,
                observed.slot.witness().attempt.checkpoint_id
            )));
        }

        match create_error {
            object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::NotFound { .. } => Err(DecisionError::Conflict(format!(
                "sink-open witness for checkpoint {} disappeared after create conflict",
                candidate.attempt.checkpoint_id
            ))),
            error => Err(DecisionError::Io(error.to_string())),
        }
    }

    /// Close exactly the supplied witness after its attempt is terminal or fully rolled back.
    ///
    /// Closure durably replaces the open state. The tombstone makes an old conditional write
    /// harmless after a successor opens and gives ambiguous responses an exact state to reconcile.
    ///
    /// # Errors
    /// Object-store I/O or a malformed, foreign, or different live witness.
    pub async fn clear_sink_open_witness(
        &self,
        expected: &CheckpointSinkOpenWitness,
    ) -> Result<(), DecisionError> {
        Self::validate_sink_open_witness_shape(expected)?;
        let deployment_id = self.load_or_create_deployment_id().await?;
        if expected.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(format!(
                "cannot clear sink-open witness from deployment {}; current deployment is \
                 {deployment_id}",
                expected.deployment_id
            )));
        }
        match self.update_mode {
            DecisionStoreUpdateMode::NativeCas => {
                self.clear_sink_open_witness_inner(expected).await
            }
            DecisionStoreUpdateMode::LocalSingleWriter => {
                let local_lock = self.local_metadata_rmw_lock.as_ref().ok_or_else(|| {
                    DecisionError::Conflict(
                        "local decision store is missing its namespace write lock".to_owned(),
                    )
                })?;
                let _guard = local_lock.lock().await;
                self.clear_sink_open_witness_inner(expected).await
            }
        }
    }

    async fn clear_sink_open_witness_inner(
        &self,
        expected: &CheckpointSinkOpenWitness,
    ) -> Result<(), DecisionError> {
        let Some(current) = self.read_sink_open_witness_record().await? else {
            return Err(DecisionError::Conflict(format!(
                "sink-open witness slot for checkpoint {} is missing",
                expected.attempt.checkpoint_id
            )));
        };
        Self::validate_sink_open_witness_slot_deployment(&current.slot, &expected.deployment_id)?;
        match &current.slot.state {
            CheckpointSinkOpenWitnessSlotState::Closed { witness, .. } if witness == expected => {
                return Ok(());
            }
            CheckpointSinkOpenWitnessSlotState::Open { witness } if witness == expected => {}
            _ => {
                return Err(DecisionError::Conflict(format!(
                    "cannot clear sink-open witness for checkpoint {}; current slot names \
                     checkpoint {} with a different create identity",
                    expected.attempt.checkpoint_id,
                    current.slot.witness().attempt.checkpoint_id
                )));
            }
        }

        let closed = CheckpointSinkOpenWitnessSlot::closed(expected.clone());
        let close_error = match self
            .put_sink_open_witness_slot(&closed, Some(current.update_version))
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        match self.read_sink_open_witness_record().await? {
            Some(observed) if observed.slot == closed => Ok(()),
            Some(observed) => {
                Self::validate_sink_open_witness_slot_deployment(
                    &observed.slot,
                    &expected.deployment_id,
                )?;
                match &observed.slot.state {
                    CheckpointSinkOpenWitnessSlotState::Closed { witness, .. }
                        if witness == expected =>
                    {
                        // Another exact close is equivalent even though its token differs.
                        Ok(())
                    }
                    CheckpointSinkOpenWitnessSlotState::Open { witness } if witness == expected => {
                        Err(DecisionError::Io(close_error.to_string()))
                    }
                    _ => {
                        // A different valid generation can only follow a successful close CAS.
                        // The stale transition cannot touch it because its object version differs.
                        Ok(())
                    }
                }
            }
            None => Err(DecisionError::Conflict(format!(
                "sink-open witness slot disappeared while closing checkpoint {}: {close_error}",
                expected.attempt.checkpoint_id
            ))),
        }
    }
}
