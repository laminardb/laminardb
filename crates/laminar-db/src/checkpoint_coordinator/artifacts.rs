use super::{
    checkpoint_artifact_identity_sha256, checkpoint_manifest_bytes, require_canonical_attempt, Arc,
    BTreeMap, Bytes, CheckpointArtifactInventory, CheckpointArtifactInventoryUpdateResult,
    CheckpointAttempt, CheckpointCoordinator, CheckpointManifest, CheckpointPhase, CheckpointScope,
    CommittedCheckpointRef, DbError, LeaderProof, PackedCheckpoint, StateChunkId, StreamExt,
    MAX_RETENTION_IO_CONCURRENCY,
};

#[derive(Clone, Copy)]
pub(super) enum AbortedSinkCleanup {
    LiveRollback,
    Recover { fencing_token: u64 },
}

struct SealedParticipantManifests {
    loaded: BTreeMap<u64, (CheckpointManifest, Bytes)>,
    sink_cleanup_complete: bool,
}

impl CheckpointCoordinator {
    pub(super) fn checkpoint_artifact_inventory(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
    ) -> Result<CheckpointArtifactInventory, DbError> {
        if assignment_fence.is_none()
            && self.store.participant_id() != laminar_core::state::LOCAL_NODE_ID.0
        {
            return Err(DbError::Checkpoint(
                "local checkpoint artifacts require the singleton local participant".into(),
            ));
        }
        let inventory = CheckpointArtifactInventory {
            deployment_id: self.expected_deployment_id()?.to_owned(),
            pipeline_identity: self.expected_pipeline_identity()?,
            attempt: require_canonical_attempt(attempt, "checkpoint artifact admission")?,
            assignment_fence,
        };
        inventory.validate().map_err(DbError::Checkpoint)?;
        Ok(inventory)
    }

    pub(super) fn validate_checkpoint_artifact_inventory(
        &self,
        inventory: &CheckpointArtifactInventory,
    ) -> Result<(), DbError> {
        inventory.validate().map_err(DbError::Checkpoint)?;
        if inventory.deployment_id != self.expected_deployment_id()?
            || inventory.pipeline_identity != self.expected_pipeline_identity()?
        {
            return Err(DbError::Checkpoint(
                "checkpoint artifact inventory does not belong to this pipeline deployment".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn begin_checkpoint_artifacts_until(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<&LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let inventory = self.checkpoint_artifact_inventory(attempt, assignment_fence)?;

        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            let proof = leader_proof.ok_or_else(|| {
                DbError::Checkpoint("cluster artifact admission has no leader proof".into())
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            let admitted = tokio::time::timeout_at(
                deadline,
                authority.begin_cluster_checkpoint_artifacts(proof, inventory.clone()),
            )
            .await
            .map_err(|_| DbError::Checkpoint("cluster artifact admission timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("cluster artifact admission failed: {error}"))
            })?;
            if admitted != inventory {
                return Err(DbError::Checkpoint(
                    "cluster artifact admission returned a different inventory".into(),
                ));
            }
            return Ok(());
        }

        if inventory.assignment_fence.is_some() || leader_proof.is_some() {
            return Err(DbError::Checkpoint(
                "local artifact admission cannot carry cluster authority".into(),
            ));
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("checkpoint artifact admission requires a decision store".into())
        })?;
        let result = tokio::time::timeout_at(
            deadline,
            store.begin_checkpoint_artifact_inventory(inventory),
        )
        .await
        .map_err(|_| DbError::Checkpoint("checkpoint artifact admission timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!("checkpoint artifact admission failed: {error}"))
        })?;
        match result {
            CheckpointArtifactInventoryUpdateResult::Applied
            | CheckpointArtifactInventoryUpdateResult::Unchanged => Ok(()),
            CheckpointArtifactInventoryUpdateResult::Conflict { current } => {
                Err(DbError::Checkpoint(format!(
                    "checkpoint artifact admission conflicts with {current:?}"
                )))
            }
        }
    }

    pub(super) async fn persist_checkpoint_until(
        &mut self,
        packed: &PackedCheckpoint,
        deadline: tokio::time::Instant,
    ) -> Result<Bytes, DbError> {
        self.phase = CheckpointPhase::Persisting;
        if tokio::time::Instant::now() >= deadline {
            self.retain_ambiguous_prepared(packed)?;
            return Err(DbError::Checkpoint(
                "checkpoint persistence timed out".into(),
            ));
        }
        let persisted = tokio::time::timeout_at(
            deadline,
            self.store
                .save_checkpoint(&packed.manifest, &packed.node_data),
        )
        .await;
        let manifest_bytes = match persisted {
            Err(_) => {
                self.retain_ambiguous_prepared(packed)?;
                return Err(DbError::Checkpoint(
                    "checkpoint persistence timed out".into(),
                ));
            }
            Ok(Err(error)) => {
                self.retain_ambiguous_prepared(packed)?;
                return Err(DbError::from(error));
            }
            Ok(Ok(bytes)) => bytes,
        };
        self.total_bytes_written = self
            .total_bytes_written
            .saturating_add(packed.manifest.node_data.object_length);
        self.prepared.insert(
            CheckpointAttempt::canonical(packed.manifest.checkpoint_id),
            (Arc::new(packed.manifest.clone()), manifest_bytes.clone()),
        );
        Ok(manifest_bytes)
    }

    fn retain_ambiguous_prepared(&mut self, packed: &PackedCheckpoint) -> Result<(), DbError> {
        let manifest_bytes = Bytes::from(checkpoint_manifest_bytes(&packed.manifest).map_err(
            |error| DbError::Checkpoint(format!("encode checkpoint manifest: {error}")),
        )?);
        self.prepared
            .entry(CheckpointAttempt::canonical(packed.manifest.checkpoint_id))
            .or_insert_with(|| (Arc::new(packed.manifest.clone()), manifest_bytes));
        Ok(())
    }

    async fn seal_checkpoint_artifacts_until(
        &mut self,
        inventory: &CheckpointArtifactInventory,
        predecessor: Option<CommittedCheckpointRef>,
        sink_cleanup: AbortedSinkCleanup,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.validate_checkpoint_artifact_inventory(inventory)?;
        let participant_ids = inventory.assignment_fence.as_ref().map_or_else(
            || vec![laminar_core::state::LOCAL_NODE_ID.0],
            laminar_core::checkpoint::CheckpointAssignmentFence::participant_ids,
        );
        let sealed = self
            .seal_participant_manifests_until(inventory, &participant_ids, deadline)
            .await?;
        if !sealed.sink_cleanup_complete {
            if let AbortedSinkCleanup::Recover { fencing_token } = sink_cleanup {
                let manifests = sealed
                    .loaded
                    .values()
                    .map(|(manifest, _)| manifest)
                    .collect::<Vec<_>>();
                self.cleanup_aborted_external_sinks_until(
                    inventory.attempt,
                    &manifests,
                    fencing_token,
                    deadline,
                )
                .await?;
            }
            self.complete_participant_sink_cleanup_until(inventory, &participant_ids, deadline)
                .await?;
        }
        self.seal_candidate_index_if_complete_until(
            inventory,
            predecessor,
            participant_ids.len(),
            sealed.loaded,
            deadline,
        )
        .await?;
        self.seal_participant_node_data_until(inventory, &participant_ids, deadline)
            .await?;
        self.prepared.remove(&inventory.attempt);
        Ok(())
    }

    async fn seal_participant_manifests_until(
        &self,
        inventory: &CheckpointArtifactInventory,
        participant_ids: &[u64],
        deadline: tokio::time::Instant,
    ) -> Result<SealedParticipantManifests, DbError> {
        let mut manifest_seals = futures::stream::iter(participant_ids.iter().copied())
            .map(|participant_id| {
                let store = Arc::clone(&self.store);
                let chunk = StateChunkId {
                    participant_id,
                    checkpoint_id: inventory.attempt.checkpoint_id,
                };
                let artifact_identity =
                    checkpoint_artifact_identity_sha256(inventory, chunk).map_err(DbError::from);
                async move {
                    let artifact_identity = artifact_identity?;
                    let seal = tokio::time::timeout_at(deadline, async {
                        store.seal_aborted_manifest(chunk, &artifact_identity).await
                    })
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "participant {participant_id} artifact manifest seal timed out"
                        ))
                    })?
                    .map_err(DbError::from)?;
                    Ok::<_, DbError>((participant_id, seal))
                }
            })
            .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY);
        let mut loaded = BTreeMap::new();
        let mut sink_cleanup_complete = true;
        while let Some(result) = manifest_seals.next().await {
            let (participant_id, seal) = result?;
            sink_cleanup_complete &= seal.sink_cleanup_complete;
            if let Some((manifest, encoded)) = seal.original_manifest {
                if manifest.deployment_id != inventory.deployment_id
                    || manifest.pipeline_identity != inventory.pipeline_identity
                    || manifest.epoch != inventory.attempt.epoch
                    || manifest.checkpoint_id != inventory.attempt.checkpoint_id
                    || manifest.participant_id != participant_id
                    || manifest.vnode_count != self.store.key_group_count().get()
                    || manifest.node_data.chunk
                        != (StateChunkId {
                            participant_id,
                            checkpoint_id: inventory.attempt.checkpoint_id,
                        })
                    || manifest.assignment_fence != inventory.assignment_fence
                {
                    return Err(DbError::Checkpoint(format!(
                        "participant {participant_id} manifest does not match the active artifact inventory"
                    )));
                }
                loaded.insert(participant_id, (manifest, encoded));
            }
        }
        Ok(SealedParticipantManifests {
            loaded,
            sink_cleanup_complete,
        })
    }

    async fn complete_participant_sink_cleanup_until(
        &self,
        inventory: &CheckpointArtifactInventory,
        participant_ids: &[u64],
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let mut completions = futures::stream::iter(participant_ids.iter().copied())
            .map(|participant_id| {
                let store = Arc::clone(&self.store);
                let chunk = StateChunkId {
                    participant_id,
                    checkpoint_id: inventory.attempt.checkpoint_id,
                };
                let artifact_identity =
                    checkpoint_artifact_identity_sha256(inventory, chunk).map_err(DbError::from);
                async move {
                    let artifact_identity = artifact_identity?;
                    let seal = tokio::time::timeout_at(deadline, async {
                        store
                            .complete_aborted_sink_cleanup(chunk, &artifact_identity)
                            .await
                    })
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "participant {participant_id} sink cleanup marker timed out"
                        ))
                    })?
                    .map_err(DbError::from)?;
                    if !seal.sink_cleanup_complete {
                        return Err(DbError::Checkpoint(format!(
                            "participant {participant_id} sink cleanup marker was not durable"
                        )));
                    }
                    Ok(())
                }
            })
            .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY);
        while let Some(result) = completions.next().await {
            result?;
        }
        Ok(())
    }

    async fn seal_candidate_index_if_complete_until(
        &self,
        inventory: &CheckpointArtifactInventory,
        predecessor: Option<CommittedCheckpointRef>,
        participant_count: usize,
        loaded: BTreeMap<u64, (CheckpointManifest, Bytes)>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if loaded.len() != participant_count {
            return Ok(());
        }
        let manifests = loaded.into_values().collect::<Vec<_>>();
        let scope = if inventory.assignment_fence.is_some() {
            CheckpointScope::Cluster
        } else {
            CheckpointScope::Local
        };
        let predecessor_source_watermarks = self
            .predecessor_source_watermarks_until(predecessor.as_ref(), deadline)
            .await?;
        let candidate = self.build_committed_index(
            inventory.attempt,
            scope,
            inventory.assignment_fence.clone(),
            predecessor,
            &predecessor_source_watermarks,
            &manifests,
            None,
        )?;
        let decisions = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("artifact sealing requires a decision store".into())
        })?;
        tokio::time::timeout_at(
            deadline,
            decisions.seal_aborted_committed_checkpoint_candidate(&candidate),
        )
        .await
        .map_err(|_| DbError::Checkpoint("candidate checkpoint index seal timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!("candidate checkpoint index seal failed: {error}"))
        })
    }

    async fn seal_participant_node_data_until(
        &self,
        inventory: &CheckpointArtifactInventory,
        participant_ids: &[u64],
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let mut data_seals = futures::stream::iter(participant_ids.iter().copied())
            .map(|participant_id| {
                let store = Arc::clone(&self.store);
                let chunk = StateChunkId {
                    participant_id,
                    checkpoint_id: inventory.attempt.checkpoint_id,
                };
                let artifact_identity =
                    checkpoint_artifact_identity_sha256(inventory, chunk).map_err(DbError::from);
                async move {
                    let artifact_identity = artifact_identity?;
                    tokio::time::timeout_at(deadline, async {
                        store
                            .seal_aborted_node_data(chunk, &artifact_identity)
                            .await
                    })
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "participant {participant_id} node-data seal timed out"
                        ))
                    })?
                    .map_err(DbError::from)
                }
            })
            .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY);
        while let Some(result) = data_seals.next().await {
            result?;
        }
        Ok(())
    }
    pub(super) async fn cleanup_local_checkpoint_artifacts_until(
        &mut self,
        attempt: CheckpointAttempt,
        sink_cleanup: AbortedSinkCleanup,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let store = Arc::clone(self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("local artifact cleanup requires a decision store".into())
        })?);
        let Some(head) = tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
            .await
            .map_err(|_| DbError::Checkpoint("local artifact cleanup read timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("local artifact cleanup read failed: {error}"))
            })?
        else {
            return Ok(());
        };
        let Some(inventory) = head.active_artifacts else {
            return Ok(());
        };
        if inventory.attempt != attempt || inventory.assignment_fence.is_some() {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} does not match the active local artifact inventory",
                attempt.checkpoint_id
            )));
        }
        if !head.latest_terminal.as_ref().is_some_and(|outcome| {
            !outcome.is_commit()
                && outcome.epoch == attempt.epoch
                && outcome.checkpoint_id == attempt.checkpoint_id
        }) {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} artifacts cannot be cleaned without its durable Abort",
                attempt.checkpoint_id
            )));
        }
        let predecessor = head
            .latest_commit
            .and_then(|outcome| outcome.committed_checkpoint);
        self.seal_checkpoint_artifacts_until(&inventory, predecessor, sink_cleanup, deadline)
            .await?;
        let result = tokio::time::timeout_at(
            deadline,
            store.complete_checkpoint_artifact_cleanup(&inventory),
        )
        .await
        .map_err(|_| DbError::Checkpoint("local artifact inventory cleanup timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!("local artifact inventory cleanup failed: {error}"))
        })?;
        match result {
            CheckpointArtifactInventoryUpdateResult::Applied
            | CheckpointArtifactInventoryUpdateResult::Unchanged => Ok(()),
            CheckpointArtifactInventoryUpdateResult::Conflict { current } => {
                Err(DbError::Checkpoint(format!(
                    "local artifact inventory cleanup conflicted with {current:?}"
                )))
            }
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn settle_cluster_checkpoint_artifacts_until(
        &mut self,
        proof: &LeaderProof,
        deadline: tokio::time::Instant,
    ) -> Result<bool, DbError> {
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster artifact cleanup has no cluster controller".into())
        })?;
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
        })?;
        let Some(inventory) =
            tokio::time::timeout_at(deadline, authority.cluster_checkpoint_artifacts())
                .await
                .map_err(|_| {
                    DbError::Checkpoint("cluster artifact inventory read timed out".into())
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!("cluster artifact inventory read failed: {error}"))
                })?
        else {
            return Ok(false);
        };
        self.validate_checkpoint_artifact_inventory(&inventory)?;
        let assignment_fence = inventory.assignment_fence.clone().ok_or_else(|| {
            DbError::Checkpoint("cluster artifact inventory has no assignment fence".into())
        })?;
        let settlement = tokio::time::timeout_at(
            deadline,
            authority.cluster_attempt_settlement(inventory.attempt),
        )
        .await
        .map_err(|_| DbError::Checkpoint("cluster artifact settlement read timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!("cluster artifact settlement read failed: {error}"))
        })?;
        match settlement {
            None => {
                self.record_outcome_until(
                    inventory.attempt,
                    laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
                    None,
                    Some(assignment_fence),
                    Some(proof.clone()),
                    deadline,
                )
                .await?;
            }
            Some(outcome)
                if outcome.epoch == inventory.attempt.epoch
                    && outcome.checkpoint_id == inventory.attempt.checkpoint_id
                    && !outcome.is_commit()
                    && outcome.deployment_id == inventory.deployment_id
                    && outcome.scope == CheckpointScope::Cluster
                    && outcome.assignment_fence.as_ref() == inventory.assignment_fence.as_ref() => {
            }
            Some(_) => {
                return Err(DbError::Checkpoint(format!(
                    "checkpoint {} has incompatible terminal authority while artifacts remain",
                    inventory.attempt.checkpoint_id
                )));
            }
        }
        let latest_commit =
            tokio::time::timeout_at(deadline, authority.highest_cluster_committed_outcome())
                .await
                .map_err(|_| {
                    DbError::Checkpoint("cluster committed predecessor read timed out".into())
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "cluster committed predecessor read failed: {error}"
                    ))
                })?;
        if latest_commit
            .as_ref()
            .is_some_and(|outcome| outcome.epoch >= inventory.attempt.epoch)
        {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} artifact cleanup does not follow the committed head",
                inventory.attempt.checkpoint_id
            )));
        }
        let predecessor = latest_commit.and_then(|outcome| outcome.committed_checkpoint);
        self.seal_checkpoint_artifacts_until(
            &inventory,
            predecessor,
            AbortedSinkCleanup::Recover {
                fencing_token: proof.fencing_token,
            },
            deadline,
        )
        .await?;
        tokio::time::timeout_at(
            deadline,
            authority.finish_cluster_checkpoint_artifact_cleanup(proof, &inventory),
        )
        .await
        .map_err(|_| DbError::Checkpoint("cluster artifact inventory cleanup timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "cluster artifact inventory cleanup failed: {error}"
            ))
        })?;
        Ok(true)
    }
}
