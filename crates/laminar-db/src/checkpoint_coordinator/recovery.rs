use std::sync::Arc;

use laminar_core::checkpoint::{
    classify_channel_progress, CheckpointAttempt, CheckpointManifest, CheckpointScope,
    CheckpointWatermark, CommittedCheckpointIndex, LeaderProof, PipelineIdentity,
};
use laminar_core::checkpoint_decision::{
    CheckpointArtifactInventory, CheckpointDecisionHead, CheckpointDecisionStore, CheckpointOutcome,
};
#[cfg(feature = "cluster")]
use laminar_core::cluster::control::ClusterController;
#[cfg(feature = "cluster")]
use std::collections::BTreeMap;

use super::{checked_successor_epoch, CheckpointCoordinator, CheckpointPhase};
use crate::error::DbError;
use crate::recovery_manager::{ClusterRecoveryTarget, RecoveredState, RecoveryManager};

#[cfg(feature = "cluster")]
pub(super) fn recovery_sink_fence(
    checkpoint_proof: Option<&LeaderProof>,
    continuation_proof: Option<&LeaderProof>,
) -> Result<Option<u64>, DbError> {
    let Some(continuation_proof) = continuation_proof else {
        return Ok(None);
    };
    if !continuation_proof.is_canonical() {
        return Err(DbError::Checkpoint(
            "cluster recovery continuation has a non-canonical leader proof".into(),
        ));
    }
    let checkpoint_proof = checkpoint_proof
        .filter(|proof| proof.is_canonical())
        .ok_or_else(|| {
            DbError::Checkpoint(
                "cluster recovery checkpoint has no canonical publishing leader proof".into(),
            )
        })?;
    if continuation_proof.fencing_token < checkpoint_proof.fencing_token {
        return Err(DbError::Checkpoint(format!(
            "cluster recovery leader fencing token {} regressed below checkpoint token {}",
            continuation_proof.fencing_token, checkpoint_proof.fencing_token
        )));
    }

    // RECOVERY: the immutable checkpoint outcome binds the exact Delta publication batch to the
    // leader term that committed it. A successor is the designated reconciler, but changing the
    // batch token would make an already-published exact cursor look conflicting instead of
    // idempotent after failover.
    Ok(Some(checkpoint_proof.fencing_token))
}

impl CheckpointCoordinator {
    #[cfg(feature = "cluster")]
    pub(crate) fn set_recovery_graph_payload_limit(&mut self, bytes: usize) {
        debug_assert_ne!(bytes, 0);
        self.recovery_graph_payload_limit = bytes;
    }

    fn recovery_scope(&self) -> CheckpointScope {
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return CheckpointScope::Cluster;
        }
        CheckpointScope::Local
    }

    fn manifest_watermark(
        manifest: Option<&CheckpointManifest>,
    ) -> Result<CheckpointWatermark, DbError> {
        let Some(manifest) = manifest else {
            return Ok(CheckpointWatermark::Uninitialized);
        };
        classify_channel_progress(&manifest.channel_progress)
            .map_err(|error| DbError::Checkpoint(format!("recovered channel progress: {error}")))
    }

    #[cfg(feature = "cluster")]
    fn recovery_target(
        &self,
        recovery_scope: CheckpointScope,
    ) -> Result<Option<ClusterRecoveryTarget>, DbError> {
        if recovery_scope == CheckpointScope::Cluster {
            let controller = self.cluster_controller.as_ref().ok_or_else(|| {
                DbError::Checkpoint("cluster recovery has no cluster controller".into())
            })?;
            let assignment = controller
                .checkpoint_assignment_fence(self.assignment_version)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "cluster recovery has no active assignment fence for version {}",
                        self.assignment_version
                    ))
                })?;
            return Ok(Some(ClusterRecoveryTarget {
                assignment,
                owned_vnodes: self.owned_vnodes.clone(),
                max_graph_payload_bytes: self.recovery_graph_payload_limit,
            }));
        }
        Ok(None)
    }

    #[cfg(feature = "cluster")]
    async fn validate_recovery_target_until(
        &self,
        outcome: &CheckpointOutcome,
        committed: &CommittedCheckpointIndex,
        target: &ClusterRecoveryTarget,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let predecessor = committed.assignment_fence.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster recovery checkpoint has no assignment fence".into())
        })?;
        if predecessor == &target.assignment {
            return Ok(());
        }
        if predecessor.assignment_version >= target.assignment.assignment_version
            || predecessor.vnode_count != target.assignment.vnode_count
            || predecessor.partitioning_abi_version != target.assignment.partitioning_abi_version
            || !committed.reassignment_portable
        {
            return Err(DbError::Checkpoint(format!(
                "recovery target assignment {} is not a compatible newer bootstrap target for committed assignment {}",
                target.assignment.assignment_version, predecessor.assignment_version
            )));
        }
        let expected = outcome.committed_checkpoint.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "cluster Commit outcome has no committed checkpoint reference".into(),
            )
        })?;
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster recovery has no cluster controller".into())
        })?;
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
        })?;
        let pinned = tokio::time::timeout_at(
            deadline,
            authority.assignment_handoff_checkpoint(&target.assignment),
        )
        .await
        .map_err(|_| DbError::Checkpoint("assignment handoff checkpoint lookup timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "assignment handoff checkpoint lookup failed: {error}"
            ))
        })?;
        if pinned.as_ref() != Some(expected) {
            return Err(DbError::Checkpoint(
                "recovery checkpoint is not the durable handoff pin for the active assignment"
                    .into(),
            ));
        }
        Ok(())
    }

    async fn load_recovered_state_until(
        &self,
        outcome: &CheckpointOutcome,
        committed: &CommittedCheckpointIndex,
        pipeline_identity: &PipelineIdentity,
        deployment_id: &str,
        recovery_scope: CheckpointScope,
        cluster_target: Option<ClusterRecoveryTarget>,
        deadline: tokio::time::Instant,
    ) -> Result<RecoveredState, DbError> {
        let manager = RecoveryManager::new(
            self.store.as_ref(),
            pipeline_identity,
            deployment_id,
            recovery_scope,
        );
        tokio::time::timeout_at(
            deadline,
            manager.recover_committed_for_target(outcome, committed, cluster_target),
        )
        .await
        .map_err(|_| DbError::Checkpoint("checkpoint recovery timed out".into()))?
    }

    async fn continue_recovered_sinks_until(
        &mut self,
        outcome: &CheckpointOutcome,
        committed: &CommittedCheckpointIndex,
        recovered: &RecoveredState,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        #[cfg(not(feature = "cluster"))]
        let _ = outcome;
        #[cfg(feature = "cluster")]
        let continuation_proof = if committed.scope == CheckpointScope::Cluster {
            self.cluster_controller
                .as_ref()
                .and_then(|controller| controller.capture_leader_proof())
        } else {
            None
        };
        #[cfg(not(feature = "cluster"))]
        let continuation_proof: Option<LeaderProof> = None;
        let continuation_fencing_token = match committed.scope {
            CheckpointScope::Local => Some(1),
            CheckpointScope::Cluster => {
                #[cfg(feature = "cluster")]
                {
                    recovery_sink_fence(outcome.leader_proof.as_ref(), continuation_proof.as_ref())?
                }
                #[cfg(not(feature = "cluster"))]
                {
                    None
                }
            }
        };
        if let Some(fencing_token) = continuation_fencing_token {
            let manifests = recovered.manifests.iter().collect::<Vec<_>>();
            self.commit_external_sinks_until(
                CheckpointAttempt::canonical(committed.checkpoint_id),
                &manifests,
                fencing_token,
                committed
                    .predecessor
                    .as_ref()
                    .map_or(0, |reference| reference.checkpoint_id),
                deadline,
            )
            .await?;
            self.schedule_retention(committed.clone(), continuation_proof.as_ref());
        }
        Ok(())
    }

    fn install_recovered_metadata(
        &mut self,
        outcome: &CheckpointOutcome,
        committed: &CommittedCheckpointIndex,
        recovered: &RecoveredState,
    ) -> Result<(), DbError> {
        let reference = outcome.committed_checkpoint.clone().ok_or_else(|| {
            DbError::Checkpoint("Commit outcome has no committed checkpoint reference".into())
        })?;
        let committed_source_watermarks = committed
            .effective_source_watermarks()
            .map_err(DbError::Checkpoint)?;
        let local_manifest = (!recovered.reassigned)
            .then(|| {
                recovered
                    .manifests
                    .iter()
                    .find(|manifest| manifest.participant_id == self.store.participant_id())
                    .cloned()
                    .map(Arc::new)
            })
            .flatten();
        self.local_watermark = Self::manifest_watermark(local_manifest.as_deref())?;
        self.last_committed_manifest = local_manifest;
        self.last_committed_ref = Some(reference.clone());
        self.last_committed_source_watermarks =
            Some((reference, committed_source_watermarks.clone()));
        self.prepared.clear();
        self.allocator.advance_epoch_to(checked_successor_epoch(
            committed.epoch,
            "installing recovered checkpoint",
        )?);
        self.failure_requires_recovery = false;
        self.phase = CheckpointPhase::Idle;
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            controller
                .replace_recovered_checkpoint_progress(
                    &committed.channel_progress,
                    &committed_source_watermarks,
                )
                .map_err(DbError::Checkpoint)?;
        }
        Ok(())
    }

    async fn install_recovered_cut(
        &mut self,
        outcome: CheckpointOutcome,
        committed: CommittedCheckpointIndex,
        deadline: tokio::time::Instant,
    ) -> Result<RecoveredState, DbError> {
        let pipeline_identity = self.expected_pipeline_identity()?;
        let deployment_id = self.expected_deployment_id()?.to_owned();
        let recovery_scope = self.recovery_scope();
        #[cfg(feature = "cluster")]
        let cluster_target = self.recovery_target(recovery_scope)?;
        #[cfg(not(feature = "cluster"))]
        let cluster_target = None;
        #[cfg(feature = "cluster")]
        if let Some(target) = cluster_target.as_ref() {
            self.validate_recovery_target_until(&outcome, &committed, target, deadline)
                .await?;
        }
        let recovered = self
            .load_recovered_state_until(
                &outcome,
                &committed,
                &pipeline_identity,
                &deployment_id,
                recovery_scope,
                cluster_target,
                deadline,
            )
            .await?;
        self.continue_recovered_sinks_until(&outcome, &committed, &recovered, deadline)
            .await?;
        self.install_recovered_metadata(&outcome, &committed, &recovered)?;
        Ok(recovered)
    }
}

#[cfg(feature = "cluster")]
async fn select_cluster_recovery_cut_until(
    controller: &ClusterController,
    deadline: tokio::time::Instant,
) -> Result<Option<(CheckpointOutcome, CommittedCheckpointIndex)>, DbError> {
    let authority = controller
        .checkpoint_authority()
        .map_err(|error| DbError::Checkpoint(format!("cluster checkpoint authority: {error}")))?;
    let Some(selected) =
        tokio::time::timeout_at(deadline, authority.highest_cluster_committed_outcome())
            .await
            .map_err(|_| DbError::Checkpoint("cluster recovery selection timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("cluster recovery selection: {error}")))?
    else {
        return Ok(None);
    };
    let (outcome, committed) = tokio::time::timeout_at(
        deadline,
        authority.cluster_outcome_with_committed_checkpoint(selected.epoch),
    )
    .await
    .map_err(|_| DbError::Checkpoint("cluster checkpoint read timed out".into()))?
    .map_err(|error| DbError::Checkpoint(format!("cluster checkpoint read: {error}")))?
    .ok_or_else(|| DbError::Checkpoint("selected cluster checkpoint disappeared".into()))?;
    if outcome != selected {
        return Err(DbError::Checkpoint(
            "cluster recovery selection changed during exact read".into(),
        ));
    }
    let committed = committed.ok_or_else(|| {
        DbError::Checkpoint("selected cluster Commit has no checkpoint index".into())
    })?;
    Ok(Some((outcome, committed)))
}

async fn load_local_recovery_head_until(
    store: &CheckpointDecisionStore,
    deadline: tokio::time::Instant,
) -> Result<Option<CheckpointDecisionHead>, DbError> {
    tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
        .await
        .map_err(|_| DbError::Checkpoint("checkpoint recovery selection timed out".into()))?
        .map_err(|error| DbError::Checkpoint(format!("checkpoint recovery selection: {error}")))
}

impl CheckpointCoordinator {
    async fn settle_local_checkpoint_artifacts_until(
        &mut self,
        head: &CheckpointDecisionHead,
        inventory: &CheckpointArtifactInventory,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.validate_checkpoint_artifact_inventory(inventory)?;
        if inventory.assignment_fence.is_some() {
            return Err(DbError::Checkpoint(
                "local recovery found cluster artifact inventory".into(),
            ));
        }
        let exact_abort = head.latest_terminal.as_ref().is_some_and(|outcome| {
            !outcome.is_commit()
                && outcome.epoch == inventory.attempt.epoch
                && outcome.checkpoint_id == inventory.attempt.checkpoint_id
        });
        if !exact_abort {
            if head
                .latest_terminal
                .as_ref()
                .is_some_and(|outcome| outcome.epoch >= inventory.attempt.epoch)
            {
                return Err(DbError::Checkpoint(format!(
                    "checkpoint {} has incompatible terminal authority while artifacts remain",
                    inventory.attempt.checkpoint_id
                )));
            }
            self.record_outcome_until(
                inventory.attempt,
                laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
                None,
                None,
                None,
                deadline,
            )
            .await?;
        }
        self.cleanup_local_checkpoint_artifacts_until(inventory.attempt, deadline)
            .await
    }

    async fn recover_local_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<RecoveredState>, DbError> {
        let store = Arc::clone(self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("checkpoint recovery requires a decision store".into())
        })?);
        let mut head = load_local_recovery_head_until(store.as_ref(), deadline).await?;
        if let Some(current) = head.as_ref() {
            if let Some(inventory) = current.active_artifacts.clone() {
                self.settle_local_checkpoint_artifacts_until(current, &inventory, deadline)
                    .await?;
                head = load_local_recovery_head_until(store.as_ref(), deadline).await?;
            }
        }
        let Some(outcome) = head.and_then(|head| head.latest_commit) else {
            return Ok(None);
        };
        let reference = outcome.committed_checkpoint.as_ref().ok_or_else(|| {
            DbError::Checkpoint("selected Commit has no checkpoint index reference".into())
        })?;
        let committed =
            tokio::time::timeout_at(deadline, store.load_committed_checkpoint(reference))
                .await
                .map_err(|_| DbError::Checkpoint("committed checkpoint read timed out".into()))?
                .map_err(|error| {
                    DbError::Checkpoint(format!("committed checkpoint read: {error}"))
                })?;
        self.install_recovered_cut(outcome, committed, deadline)
            .await
            .map(Some)
    }

    pub async fn recover(&mut self) -> Result<Option<RecoveredState>, DbError> {
        if self.phase != CheckpointPhase::Idle {
            return Err(DbError::Checkpoint(
                "cannot recover while a checkpoint is in progress".into(),
            ));
        }
        let deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;

        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            let Some((outcome, committed)) =
                select_cluster_recovery_cut_until(controller.as_ref(), deadline).await?
            else {
                return Ok(None);
            };
            return self
                .install_recovered_cut(outcome, committed, deadline)
                .await
                .map(Some);
        }

        self.recover_local_until(deadline).await
    }

    #[cfg(feature = "cluster")]
    async fn recover_genesis_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<RecoveredState>, DbError> {
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "epoch-targeted recovery requires cluster checkpoint authority".into(),
            )
        })?;
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
        })?;
        if tokio::time::timeout_at(deadline, authority.cluster_checkpoint_artifacts())
            .await
            .map_err(|_| DbError::Checkpoint("genesis recovery artifact audit timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("genesis recovery artifact audit failed: {error}"))
            })?
            .is_some()
        {
            return Err(DbError::Checkpoint(
                "genesis recovery cannot install while checkpoint artifacts remain".into(),
            ));
        }
        if let Some(reference) = self
            .authoritative_committed_predecessor_until(CheckpointScope::Cluster, deadline)
            .await?
        {
            return Err(DbError::Checkpoint(format!(
                "genesis recovery cannot replace authoritative committed checkpoint {}",
                reference.checkpoint_id
            )));
        }
        self.prepared.clear();
        self.last_committed_manifest = None;
        self.last_committed_ref = None;
        self.last_committed_source_watermarks = None;
        self.local_watermark = CheckpointWatermark::Uninitialized;
        self.failure_requires_recovery = false;
        self.phase = CheckpointPhase::Idle;
        controller
            .replace_recovered_checkpoint_progress(&[], &BTreeMap::new())
            .map_err(DbError::Checkpoint)?;
        Ok(None)
    }

    #[cfg(feature = "cluster")]
    async fn recover_cluster_epoch_until(
        &mut self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<Option<RecoveredState>, DbError> {
        let authoritative = self
            .authoritative_committed_predecessor_until(CheckpointScope::Cluster, deadline)
            .await?;
        let authoritative = authoritative.ok_or_else(|| {
            DbError::Checkpoint(format!(
                "cluster epoch {epoch} cannot be recovered because no authoritative Commit exists"
            ))
        })?;
        if authoritative.epoch != epoch || authoritative.checkpoint_id != epoch {
            return Err(DbError::Checkpoint(format!(
                "cluster epoch {epoch} is not the authoritative committed recovery head {}",
                authoritative.checkpoint_id
            )));
        }
        if let Some(controller) = self.cluster_controller.as_ref() {
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            let (outcome, committed) = tokio::time::timeout_at(
                deadline,
                authority.cluster_outcome_with_committed_checkpoint(epoch),
            )
            .await
            .map_err(|_| DbError::Checkpoint("cluster checkpoint read timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("cluster checkpoint read: {error}")))?
            .ok_or_else(|| DbError::Checkpoint(format!("cluster epoch {epoch} has no outcome")))?;
            let committed = committed.ok_or_else(|| {
                DbError::Checkpoint(format!("cluster epoch {epoch} is not committed"))
            })?;
            if outcome.committed_checkpoint.as_ref() != Some(&authoritative) {
                return Err(DbError::Checkpoint(
                    "cluster recovery outcome does not match the authoritative committed head"
                        .into(),
                ));
            }
            return self
                .install_recovered_cut(outcome, committed, deadline)
                .await
                .map(Some);
        }

        Err(DbError::Checkpoint(
            "epoch-targeted recovery requires cluster checkpoint authority".into(),
        ))
    }

    #[cfg(feature = "cluster")]
    pub async fn recover_to_epoch(
        &mut self,
        epoch: u64,
    ) -> Result<Option<RecoveredState>, DbError> {
        if self.phase != CheckpointPhase::Idle {
            return Err(DbError::Checkpoint(
                "cannot recover while a checkpoint is in progress".into(),
            ));
        }
        let deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;
        if epoch == 0 {
            self.recover_genesis_until(deadline).await
        } else {
            self.recover_cluster_epoch_until(epoch, deadline).await
        }
    }
}
