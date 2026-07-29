//! Checkpoint recovery: selects a committed manifest and resolves its operator-state sidecar.
//! Runtime owners restore sources, sinks, tables, and operators from that single recovered cut.
#![allow(clippy::disallowed_types)] // cold path

#[cfg(feature = "cluster")]
use laminar_core::checkpoint::ClusterRecoveryCapsule;
use laminar_core::checkpoint_decision::{CheckpointOutcome, CheckpointScope, CheckpointVerdict};
#[cfg(feature = "cluster")]
use laminar_core::state::CheckpointAttempt;
#[cfg(feature = "cluster")]
use laminar_core::storage::checkpoint_manifest::ConnectorCheckpoint;
use laminar_core::storage::checkpoint_manifest::{
    CheckpointManifest, DurableCheckpointPhase, PipelineIdentity,
};
use laminar_core::storage::checkpoint_store::{
    CheckpointArtifacts, CheckpointStore, CheckpointStoreError,
};
use tracing::{debug, error, info, warn};

use crate::error::DbError;

#[cfg(any(feature = "cluster", test))]
pub(crate) mod vnode_chains;

/// Result of a successful recovery from a checkpoint.
#[derive(Debug)]
pub struct RecoveredState {
    /// Manifest that was restored from.
    pub manifest: CheckpointManifest,
    outcome: Option<CheckpointOutcome>,
    #[cfg(feature = "cluster")]
    cluster_capsule: Option<ClusterRecoveryCapsule>,
    #[cfg(feature = "cluster")]
    vnode_restore_cut: Option<crate::checkpoint_coordinator::ValidatedClusterVnodeRestoreCut>,
}

impl RecoveredState {
    /// Returns the recovered epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.manifest.epoch
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn outcome(&self) -> Option<&CheckpointOutcome> {
        self.outcome.as_ref()
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_cluster_capsule(&mut self, capsule: ClusterRecoveryCapsule) {
        self.cluster_capsule = Some(capsule);
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn cluster_capsule(&self) -> Option<&ClusterRecoveryCapsule> {
        self.cluster_capsule.as_ref()
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_vnode_restore_cut(
        &mut self,
        cut: crate::checkpoint_coordinator::ValidatedClusterVnodeRestoreCut,
    ) {
        self.vnode_restore_cut = Some(cut);
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn vnode_restore_cut(
        &self,
    ) -> Option<&crate::checkpoint_coordinator::ValidatedClusterVnodeRestoreCut> {
        self.vnode_restore_cut.as_ref()
    }
}

/// Loads the latest viable [`CheckpointManifest`] and resolves its external operator state.
pub struct RecoveryManager<'a> {
    store: &'a dyn CheckpointStore,
    expected_pipeline_identity: PipelineIdentity,
    expected_deployment_id: String,
    expected_outcome_scope: CheckpointScope,
}

#[derive(Clone, Copy)]
enum RecoveryOutcomeAuthority<'a> {
    Local(&'a laminar_core::checkpoint_decision::CheckpointDecisionStore),
    #[cfg(feature = "cluster")]
    Cluster {
        outcomes: &'a laminar_core::cluster::control::LeaderLeaseStore,
        capsules: &'a laminar_core::checkpoint_decision::CheckpointDecisionStore,
    },
}

#[derive(Debug, Clone, Copy)]
struct ClusterPreparedDominance {
    highest_terminal_epoch: u64,
    highest_terminal_checkpoint_id: u64,
}

#[cfg(feature = "cluster")]
fn validate_cluster_candidate_manifest_binding(
    manifest: &CheckpointManifest,
    participant: &laminar_core::checkpoint::ParticipantRecoveryRef,
    outcome: &CheckpointOutcome,
    expected_pipeline_identity: &PipelineIdentity,
    expected_deployment_id: &str,
    expected_portable_state_sha256: &str,
) -> Result<(), String> {
    if manifest.epoch != outcome.epoch
        || manifest.checkpoint_id != outcome.checkpoint_id
        || manifest.participant_id != participant.participant_id
        || manifest.pipeline_identity != *expected_pipeline_identity
        || manifest.deployment_id != expected_deployment_id
    {
        return Err("manifest does not identify the committed runtime cut".into());
    }
    let (manifest_sha256, portable_state_sha256) =
        crate::cluster_recovery_capsule::manifest_digests(manifest)
            .map_err(|error| format!("manifest is not portable: {error}"))?;
    if manifest_sha256 != participant.manifest_sha256
        || portable_state_sha256 != participant.portable_state_sha256
        || portable_state_sha256 != expected_portable_state_sha256
    {
        return Err("manifest digest does not match the recovery capsule".into());
    }
    Ok(())
}

#[cfg(feature = "cluster")]
fn declared_sidecar_len(manifest: &CheckpointManifest) -> Result<Option<u64>, String> {
    let mut ranges = manifest
        .operator_states
        .iter()
        .filter(|(_, state)| state.external)
        .map(|(name, state)| {
            let end = state
                .external_offset
                .checked_add(state.external_length)
                .ok_or_else(|| format!("operator '{name}' sidecar range overflows"))?;
            if state.external_length == 0 {
                return Err(format!(
                    "operator '{name}' has an empty external sidecar range"
                ));
            }
            Ok((state.external_offset, end, name.as_str()))
        })
        .collect::<Result<Vec<_>, String>>()?;
    if ranges.is_empty() {
        if manifest.operator_states.is_empty() && manifest.state_checksum.is_some() {
            return Err("sidecar checksum has no external operator ranges".into());
        }
        return Ok(None);
    }
    ranges.sort_unstable();
    let mut expected_offset = 0;
    for (start, end, name) in ranges {
        if start != expected_offset {
            return Err(format!(
                "operator '{name}' sidecar range starts at {start}, expected {expected_offset}"
            ));
        }
        expected_offset = end;
    }
    Ok(Some(expected_offset))
}

impl ClusterPreparedDominance {
    #[cfg(feature = "cluster")]
    fn from_outcomes(outcomes: &[CheckpointOutcome]) -> Option<Self> {
        let terminal = outcomes.last()?;
        Some(Self {
            highest_terminal_epoch: terminal.epoch,
            highest_terminal_checkpoint_id: terminal.checkpoint_id,
        })
    }
}

impl RecoveryOutcomeAuthority<'_> {
    const fn scope(self) -> CheckpointScope {
        match self {
            Self::Local(_) => CheckpointScope::Local,
            #[cfg(feature = "cluster")]
            Self::Cluster { .. } => CheckpointScope::Cluster,
        }
    }

    async fn outcome(self, epoch: u64) -> Result<Option<CheckpointOutcome>, DbError> {
        match self {
            Self::Local(store) => store
                .outcome(epoch)
                .await
                .map_err(|error| DbError::Checkpoint(error.to_string())),
            #[cfg(feature = "cluster")]
            Self::Cluster { outcomes, .. } => outcomes
                .cluster_outcome(epoch)
                .await
                .map_err(|error| DbError::Checkpoint(error.to_string())),
        }
    }

    async fn outcomes(self) -> Result<Vec<CheckpointOutcome>, DbError> {
        match self {
            Self::Local(store) => store
                .outcomes()
                .await
                .map_err(|error| DbError::Checkpoint(error.to_string())),
            #[cfg(feature = "cluster")]
            Self::Cluster { outcomes, .. } => outcomes
                .cluster_outcome_inventory()
                .await
                .map(|inventory| inventory.outcomes)
                .map_err(|error| DbError::Checkpoint(error.to_string())),
        }
    }
}

#[cfg(feature = "cluster")]
impl<'a> RecoveryOutcomeAuthority<'a> {
    const fn capsule_store(
        self,
    ) -> Option<&'a laminar_core::checkpoint_decision::CheckpointDecisionStore> {
        match self {
            Self::Local(_) => None,
            Self::Cluster { capsules, .. } => Some(capsules),
        }
    }
}

impl<'a> RecoveryManager<'a> {
    /// Create a recovery manager.
    #[must_use]
    pub fn new(store: &'a dyn CheckpointStore) -> Self {
        Self {
            store,
            expected_pipeline_identity: PipelineIdentity::empty(),
            expected_deployment_id: String::new(),
            expected_outcome_scope: CheckpointScope::Local,
        }
    }

    /// Require an exact logical topology/state-ABI identity during recovery.
    #[must_use]
    pub fn with_pipeline_identity(mut self, identity: &PipelineIdentity) -> Self {
        self.expected_pipeline_identity.clone_from(identity);
        self
    }

    /// Require the create-once checkpoint namespace incarnation during recovery.
    #[must_use]
    pub fn with_deployment_id(mut self, deployment_id: &str) -> Self {
        deployment_id.clone_into(&mut self.expected_deployment_id);
        self
    }

    /// Require durable outcomes to belong to the active local or cluster recovery domain.
    #[must_use]
    pub(crate) fn with_outcome_scope(mut self, scope: CheckpointScope) -> Self {
        self.expected_outcome_scope = scope;
        self
    }

    async fn restore_committed_outcome(
        &self,
        outcome: &CheckpointOutcome,
        authority: RecoveryOutcomeAuthority<'_>,
    ) -> Result<Option<RecoveredState>, DbError> {
        if outcome.scope != self.expected_outcome_scope {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] durable outcome scope {:?} does not match active runtime scope {:?}",
                outcome.scope, self.expected_outcome_scope
            )));
        }
        if outcome.deployment_id != self.expected_deployment_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] durable outcome deployment '{}' does not match runtime deployment '{}'",
                outcome.deployment_id, self.expected_deployment_id
            )));
        }
        match &outcome.verdict {
            CheckpointVerdict::Commit => {}
            CheckpointVerdict::Abort => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] epoch {} checkpoint {} has an Abort outcome and is not a recovery cut",
                    outcome.epoch, outcome.checkpoint_id
                )));
            }
        }

        if outcome.scope == CheckpointScope::Cluster {
            #[cfg(feature = "cluster")]
            {
                return self
                    .load_cluster_committed_outcome(
                        outcome,
                        authority.capsule_store().ok_or_else(|| {
                            DbError::Checkpoint(
                                "[LDB-6041] cluster recovery has no recovery capsule store".into(),
                            )
                        })?,
                        true,
                    )
                    .await;
            }
            #[cfg(not(feature = "cluster"))]
            {
                return Err(DbError::Checkpoint(
                    "[LDB-6041] cluster recovery requires the 'cluster' feature".into(),
                ));
            }
        }

        let local_participant = self.store.participant_id();
        if local_participant != 0 {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] local outcome recovery requires participant 0, but the checkpoint store is participant {local_participant}"
            )));
        }
        if outcome.recovery_capsule.is_some() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] local epoch {} checkpoint {} unexpectedly binds a cluster recovery capsule",
                outcome.epoch, outcome.checkpoint_id
            )));
        }

        let artifacts = self
            .store
            .load_checkpoint_artifacts_for_participant(0, outcome.checkpoint_id)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] committed epoch {} checkpoint {} participant {} artifacts are unreadable: {error}",
                    outcome.epoch, outcome.checkpoint_id, local_participant
                ))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] committed checkpoint {} is absent from participant \
                     {local_participant} recovery inventory",
                    outcome.checkpoint_id
                ))
            })?;
        let manifest = &artifacts.manifest;
        if manifest.epoch != outcome.epoch || manifest.checkpoint_id != outcome.checkpoint_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] outcome epoch {} checkpoint {} does not match participant \
                 {local_participant} manifest epoch {} checkpoint {}",
                outcome.epoch, outcome.checkpoint_id, manifest.epoch, manifest.checkpoint_id
            )));
        }
        self.try_restore(
            outcome.checkpoint_id,
            artifacts,
            Some(authority),
            0,
            Some(outcome),
        )
        .await
    }

    #[cfg(feature = "cluster")]
    async fn load_cluster_committed_outcome(
        &self,
        outcome: &CheckpointOutcome,
        decision_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
        finalize_local_manifest: bool,
    ) -> Result<Option<RecoveredState>, DbError> {
        let reference = outcome.recovery_capsule.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6041] cluster epoch {} checkpoint {} has no recovery capsule",
                outcome.epoch, outcome.checkpoint_id
            ))
        })?;
        let capsule = decision_store
            .load_recovery_capsule(reference)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] cluster epoch {} checkpoint {} recovery capsule is unreadable: {error}",
                    outcome.epoch, outcome.checkpoint_id
                ))
            })?;
        let expected_attempt = CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id);
        if capsule.attempt != expected_attempt
            || capsule.deployment_id != outcome.deployment_id
            || capsule.deployment_id != self.expected_deployment_id
            || capsule.pipeline_identity != self.expected_pipeline_identity
            || outcome.assignment_fence.as_ref() != Some(&capsule.assignment_fence)
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] cluster epoch {} checkpoint {} recovery capsule does not match the active attempt, deployment, pipeline identity, and assignment fence",
                outcome.epoch, outcome.checkpoint_id
            )));
        }

        let local_participant = self.store.participant_id();
        let mut candidates = capsule.participants.iter().collect::<Vec<_>>();
        if let Some(local_index) = candidates
            .iter()
            .position(|participant| participant.participant_id == local_participant)
        {
            candidates[..=local_index].rotate_right(1);
        }

        let mut rejected = 0_usize;
        let mut first_failure = None;
        let mut last_failure = None;
        let mut reject_candidate = |failure: String| {
            rejected += 1;
            if first_failure.is_none() {
                first_failure = Some(failure.clone());
            }
            last_failure = Some(failure);
        };
        for participant in candidates {
            let storage_participant = participant.participant_id;
            info!(
                local_participant,
                storage_participant,
                epoch = outcome.epoch,
                checkpoint_id = outcome.checkpoint_id,
                assignment_version = capsule.assignment_fence.assignment_version,
                "cluster recovery: validating participant checkpoint artifacts"
            );

            let artifacts = match self
                .store
                .load_checkpoint_artifacts_for_participant(
                    storage_participant,
                    outcome.checkpoint_id,
                )
                .await
            {
                Ok(Some(artifacts)) => artifacts,
                Ok(None) => {
                    reject_candidate(format!(
                        "participant {storage_participant} manifest is absent"
                    ));
                    warn!(
                        storage_participant,
                        checkpoint_id = outcome.checkpoint_id,
                        "cluster recovery candidate manifest is absent"
                    );
                    continue;
                }
                Err(error) => {
                    reject_candidate(format!(
                        "participant {storage_participant} artifacts are unreadable: {error}"
                    ));
                    warn!(
                        storage_participant,
                        checkpoint_id = outcome.checkpoint_id,
                        %error,
                        "cluster recovery candidate artifacts are unreadable"
                    );
                    continue;
                }
            };

            let manifest = &artifacts.manifest;
            if let Err(failure) = validate_cluster_candidate_manifest_binding(
                manifest,
                participant,
                outcome,
                &self.expected_pipeline_identity,
                &self.expected_deployment_id,
                &capsule.portable_state_sha256,
            ) {
                reject_candidate(format!("participant {storage_participant} {failure}"));
                warn!(
                    storage_participant,
                    checkpoint_id = outcome.checkpoint_id,
                    %failure,
                    "cluster recovery candidate manifest binding is invalid"
                );
                continue;
            }
            let (artifacts, validation) = artifacts
                .validate(
                    outcome.checkpoint_id,
                    storage_participant,
                    self.store.key_group_count(),
                    self.store.max_state_data_bytes(),
                )
                .await
                .map_err(DbError::from)?;
            if !validation.valid {
                reject_candidate(format!(
                    "participant {storage_participant} artifact integrity failed: {:?}",
                    validation.issues
                ));
                warn!(
                    storage_participant,
                    checkpoint_id = outcome.checkpoint_id,
                    issues = ?validation.issues,
                    "cluster recovery candidate artifact integrity failed"
                );
                continue;
            }

            let mut recovered = match Self::restore_from(artifacts) {
                Ok(recovered) => recovered,
                Err(error) => {
                    reject_candidate(format!(
                        "participant {storage_participant} sidecar cannot be resolved: {error}"
                    ));
                    warn!(
                        storage_participant,
                        checkpoint_id = outcome.checkpoint_id,
                        %error,
                        "cluster recovery candidate sidecar cannot be resolved"
                    );
                    continue;
                }
            };

            recovered.manifest.source_offsets = capsule
                .source_offsets
                .iter()
                .map(|(source, offsets)| {
                    let metadata = capsule
                        .source_metadata
                        .get(source)
                        .expect("validated recovery capsule has matching source metadata");
                    (
                        source.clone(),
                        ConnectorCheckpoint {
                            offsets: offsets.clone().into_iter().collect(),
                            metadata: metadata.clone().into_iter().collect(),
                            source_assignment_version: capsule
                                .source_assignment_versions
                                .get(source)
                                .copied(),
                        },
                    )
                })
                .collect();
            recovered.manifest.source_names = capsule.source_offsets.keys().cloned().collect();
            recovered.manifest.source_watermarks =
                capsule.source_watermarks.clone().into_iter().collect();
            recovered.manifest.watermark = capsule.recovery_watermark_frontier;
            let mut recovered = if finalize_local_manifest {
                self.complete_restore(
                    recovered,
                    Some(outcome.clone()),
                    outcome.checkpoint_id,
                    storage_participant,
                )
                .await?
            } else {
                recovered.outcome = Some(outcome.clone());
                recovered
            };
            recovered.set_cluster_capsule(capsule);
            return Ok(Some(recovered));
        }

        let failure_summary = match (first_failure, last_failure) {
            (Some(first), Some(last)) if first != last => {
                format!("; first failure: {first}; last failure: {last}")
            }
            (Some(failure), _) => format!("; failure: {failure}"),
            _ => String::new(),
        };
        Err(DbError::Checkpoint(format!(
            "[LDB-6041] committed cluster checkpoint {} has no usable participant artifact replica; {rejected} candidate(s) rejected{}",
            outcome.checkpoint_id,
            failure_summary
        )))
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn preflight_cluster_committed_outcome(
        &self,
        outcome: &CheckpointOutcome,
        decision_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    ) -> Result<RecoveredState, DbError> {
        self.load_cluster_committed_outcome(outcome, decision_store, false)
            .await?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] cluster checkpoint {} has no recoverable participant artifacts",
                    outcome.checkpoint_id
                ))
            })
    }

    #[cfg(feature = "cluster")]
    async fn preflight_cluster_candidate_metadata(
        &self,
        participant: &laminar_core::checkpoint::ParticipantRecoveryRef,
        outcome: &CheckpointOutcome,
        capsule: &ClusterRecoveryCapsule,
    ) -> Result<(), String> {
        let participant_id = participant.participant_id;
        let manifest = self
            .store
            .load_manifest_for_participant(participant_id, outcome.checkpoint_id)
            .await
            .map_err(|error| {
                format!("participant {participant_id} manifest is unreadable: {error}")
            })?
            .ok_or_else(|| format!("participant {participant_id} manifest is absent"))?;
        let validation = manifest.validate(self.store.key_group_count());
        if let Some(failure) = validation.first() {
            return Err(format!(
                "participant {participant_id} manifest metadata is invalid: {failure}"
            ));
        }
        validate_cluster_candidate_manifest_binding(
            &manifest,
            participant,
            outcome,
            &self.expected_pipeline_identity,
            &self.expected_deployment_id,
            &capsule.portable_state_sha256,
        )
        .map_err(|failure| format!("participant {participant_id} {failure}"))?;

        let Some(expected_len) = declared_sidecar_len(&manifest).map_err(|failure| {
            format!("participant {participant_id} manifest sidecar shape is invalid: {failure}")
        })?
        else {
            return Ok(());
        };
        match self
            .store
            .state_data_len_for_participant(participant_id, outcome.checkpoint_id)
            .await
            .map_err(|error| {
                format!("participant {participant_id} sidecar metadata is unreadable: {error}")
            })?
        {
            Some(actual_len) if actual_len == expected_len => Ok(()),
            Some(actual_len) => Err(format!(
                "participant {participant_id} sidecar is {actual_len} bytes; expected {expected_len}"
            )),
            None => Err(format!("participant {participant_id} sidecar is absent")),
        }
    }

    #[cfg(feature = "cluster")]
    /// Audit durable recovery metadata bound to a committed cluster capsule without reading
    /// state-sidecar or vnode payload bodies. Recovery validates those bodies before use.
    pub(crate) async fn preflight_cluster_committed_metadata(
        &self,
        outcome: &CheckpointOutcome,
        capsule: &ClusterRecoveryCapsule,
    ) -> Result<(), DbError> {
        capsule
            .validate()
            .map_err(|error| DbError::Checkpoint(format!("[LDB-6041] {error}")))?;
        let expected_attempt = CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id);
        if !outcome.is_commit()
            || outcome.scope != CheckpointScope::Cluster
            || capsule.attempt != expected_attempt
            || capsule.deployment_id != outcome.deployment_id
            || capsule.deployment_id != self.expected_deployment_id
            || capsule.pipeline_identity != self.expected_pipeline_identity
            || outcome.assignment_fence.as_ref() != Some(&capsule.assignment_fence)
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] cluster epoch {} checkpoint {} manifest preflight does not match the committed runtime cut",
                outcome.epoch, outcome.checkpoint_id
            )));
        }

        let local_participant = self.store.participant_id();
        let mut candidates = capsule.participants.iter().collect::<Vec<_>>();
        if let Some(local_index) = candidates
            .iter()
            .position(|participant| participant.participant_id == local_participant)
        {
            candidates[..=local_index].rotate_right(1);
        }
        let mut rejected = 0_usize;
        let mut last_failure = None;
        for participant in candidates {
            match self
                .preflight_cluster_candidate_metadata(participant, outcome, capsule)
                .await
            {
                Ok(()) => return Ok(()),
                Err(failure) => {
                    rejected += 1;
                    last_failure = Some(failure);
                }
            }
        }

        let failure = last_failure.map_or_else(String::new, |failure| format!("; {failure}"));
        Err(DbError::Checkpoint(format!(
            "[LDB-6041] committed cluster checkpoint {} has no usable participant manifest metadata; {rejected} candidate(s) rejected{failure}",
            outcome.checkpoint_id
        )))
    }

    #[cfg(feature = "cluster")]
    async fn validated_cluster_recovery_outcomes(
        &self,
        authority: &laminar_core::cluster::control::LeaderLeaseStore,
        capsule_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    ) -> Result<Vec<CheckpointOutcome>, DbError> {
        let inventory = authority
            .validated_cluster_outcome_inventory(|outcome| async move {
                self.preflight_cluster_committed_outcome(&outcome, capsule_store)
                    .await
                    .map(drop)
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        Ok(inventory.outcomes)
    }

    async fn settle_local_prepared_attempts(
        &self,
        decision_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
        outcomes: &[CheckpointOutcome],
    ) -> Result<(), DbError> {
        let authority_deployment = decision_store
            .load_or_create_deployment_id()
            .await
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        if !self.expected_deployment_id.is_empty()
            && self.expected_deployment_id != authority_deployment
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] local decision authority deployment '{}' does not match runtime deployment '{}'",
                authority_deployment, self.expected_deployment_id
            )));
        }
        let floor = decision_store
            .outcome_retention_boundary()
            .await
            .map_err(|error| DbError::Checkpoint(error.to_string()))?
            .before_epoch;
        let checkpoint_ids = self.store.list_ids().await.map_err(DbError::from)?;
        for checkpoint_id in checkpoint_ids {
            if outcomes
                .iter()
                .any(|outcome| outcome.checkpoint_id == checkpoint_id)
            {
                continue;
            }
            let manifest = self
                .store
                .load_by_id(checkpoint_id)
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6041] checkpoint {checkpoint_id} recovery inventory is unreadable while settling Prepared witnesses: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6041] checkpoint {checkpoint_id} disappeared while settling prepared recovery inventory"
                    ))
                })?;
            if manifest.durable_phase != DurableCheckpointPhase::Prepared || manifest.epoch < floor
            {
                continue;
            }
            if let Some(outcome) = outcomes
                .iter()
                .find(|outcome| outcome.epoch == manifest.epoch)
            {
                if outcome.checkpoint_id != manifest.checkpoint_id {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6041] prepared checkpoint {} epoch {} conflicts with durable outcome checkpoint {}",
                        manifest.checkpoint_id, manifest.epoch, outcome.checkpoint_id
                    )));
                }
                continue;
            }

            let validation = manifest.validate(self.store.key_group_count());
            if !validation.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] prepared checkpoint {} epoch {} is invalid and cannot be used to mint an Abort outcome: {}",
                    manifest.checkpoint_id,
                    manifest.epoch,
                    validation
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("; ")
                )));
            }
            if manifest.participant_id != 0
                || manifest.deployment_id != self.expected_deployment_id
                || manifest.pipeline_identity != self.expected_pipeline_identity
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6043] prepared checkpoint {} epoch {} does not belong to local participant 0 and the active deployment/pipeline identity",
                    manifest.checkpoint_id, manifest.epoch
                )));
            }

            let settled = decision_store
                .record_outcome(
                    manifest.epoch,
                    manifest.checkpoint_id,
                    CheckpointScope::Local,
                    None,
                    None,
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6041] prepared checkpoint {} epoch {} could not be durably settled as Abort: {error}",
                        manifest.checkpoint_id, manifest.epoch
                    ))
                })?;
            let outcome = match settled {
                laminar_core::checkpoint_decision::RecordOutcomeResult::Created(outcome)
                | laminar_core::checkpoint_decision::RecordOutcomeResult::Unchanged(outcome) => {
                    outcome
                }
                laminar_core::checkpoint_decision::RecordOutcomeResult::Conflict { winner } => {
                    winner
                }
            };
            if outcome.epoch != manifest.epoch || outcome.checkpoint_id != manifest.checkpoint_id {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] prepared checkpoint {} epoch {} was not settled by an exact durable terminal outcome",
                    manifest.checkpoint_id, manifest.epoch
                )));
            }
        }
        Ok(())
    }

    async fn validate_newer_durable_attempts(
        &self,
        selected: &CheckpointOutcome,
        outcomes: &[CheckpointOutcome],
        cluster_dominance: Option<ClusterPreparedDominance>,
    ) -> Result<(), DbError> {
        let checkpoint_ids = self.store.list_ids().await.map_err(DbError::from)?;
        for checkpoint_id in checkpoint_ids {
            if checkpoint_id == selected.checkpoint_id {
                continue;
            }
            let manifest = self
                .store
                .load_by_id(checkpoint_id)
                .await
                .map_err(DbError::from)?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6041] checkpoint {checkpoint_id} disappeared while auditing recovery inventory"
                    ))
                })?;
            if manifest.epoch <= selected.epoch && manifest.checkpoint_id <= selected.checkpoint_id
            {
                if cluster_dominance.is_none() {
                    continue;
                }
                if manifest.epoch < selected.epoch
                    && manifest.checkpoint_id < selected.checkpoint_id
                {
                    if manifest.durable_phase == DurableCheckpointPhase::Prepared
                        && !outcomes
                            .iter()
                            .any(|outcome| outcome.epoch == manifest.epoch)
                    {
                        self.validate_dominated_prepared_manifest(checkpoint_id, &manifest)?;
                    }
                    continue;
                }
            }
            let Some(outcome) = outcomes
                .iter()
                .find(|outcome| outcome.epoch == manifest.epoch)
            else {
                if let Some(outcome) = outcomes
                    .iter()
                    .find(|outcome| outcome.checkpoint_id == manifest.checkpoint_id)
                {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6041] checkpoint {} is reused by manifest epoch {} and terminal outcome epoch {}",
                        manifest.checkpoint_id, manifest.epoch, outcome.epoch
                    )));
                }
                let dominated = cluster_dominance.is_some_and(|boundary| {
                    manifest.epoch < boundary.highest_terminal_epoch
                        && manifest.checkpoint_id < boundary.highest_terminal_checkpoint_id
                });
                if manifest.durable_phase == DurableCheckpointPhase::Prepared && dominated {
                    self.validate_dominated_prepared_manifest(checkpoint_id, &manifest)?;
                    continue;
                }
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] newer {:?} checkpoint {} epoch {} is not settled by an exact terminal outcome; refusing to restore older checkpoint {} epoch {}",
                    manifest.durable_phase,
                    manifest.checkpoint_id,
                    manifest.epoch,
                    selected.checkpoint_id,
                    selected.epoch
                )));
            };
            let expected_verdict = match manifest.durable_phase {
                DurableCheckpointPhase::Prepared => CheckpointVerdict::Abort,
                DurableCheckpointPhase::Finalized => CheckpointVerdict::Commit,
            };
            if outcome.checkpoint_id != manifest.checkpoint_id
                || outcome.verdict != expected_verdict
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] newer {:?} checkpoint {} epoch {} conflicts with terminal outcome checkpoint {} {:?}",
                    manifest.durable_phase,
                    manifest.checkpoint_id,
                    manifest.epoch,
                    outcome.checkpoint_id,
                    outcome.verdict
                )));
            }
        }
        Ok(())
    }

    fn validate_dominated_prepared_manifest(
        &self,
        storage_id: u64,
        manifest: &CheckpointManifest,
    ) -> Result<(), DbError> {
        if manifest.checkpoint_id != storage_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] storage checkpoint {storage_id} contains dominated Prepared manifest checkpoint {}",
                manifest.checkpoint_id
            )));
        }
        let validation = manifest.validate(self.store.key_group_count());
        if !validation.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] dominated Prepared checkpoint {} epoch {} is invalid: {}",
                manifest.checkpoint_id,
                manifest.epoch,
                validation
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            )));
        }
        if manifest.participant_id != self.store.participant_id()
            || manifest.deployment_id != self.expected_deployment_id
            || manifest.pipeline_identity != self.expected_pipeline_identity
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] dominated Prepared checkpoint {} epoch {} does not belong to storage participant {} and the active deployment/pipeline identity",
                manifest.checkpoint_id,
                manifest.epoch,
                self.store.participant_id()
            )));
        }
        Ok(())
    }

    async fn validate_no_commit_outcome_genesis(&self) -> Result<(), DbError> {
        // Prepared-only inventory is normal residue from an aborted or unresolved attempt. A
        // Finalized manifest, torn pointer, or unreadable inventory is different: it is evidence
        // that authoritative Commit history was lost, so genesis would replay visible output.
        let published = self.store.load_latest().await.map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6041] checkpoint recovery pointer is invalid while no durable Commit outcome exists: {error}"
            ))
        })?;
        if let Some(manifest) = published {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] published finalized checkpoint {} epoch {} exists but no durable Commit outcome exists",
                manifest.checkpoint_id, manifest.epoch
            )));
        }
        let checkpoint_ids = self.store.list_ids().await.map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6041] recovery inventory cannot be enumerated while no durable Commit outcome exists: {error}"
            ))
        })?;
        for checkpoint_id in checkpoint_ids {
            let manifest = self
                .store
                .load_by_id(checkpoint_id)
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6041] checkpoint {checkpoint_id} recovery inventory is unreadable while no durable Commit outcome exists: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6041] checkpoint {checkpoint_id} disappeared from recovery inventory while no durable Commit outcome exists"
                    ))
                })?;
            if manifest.checkpoint_id != checkpoint_id {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] storage checkpoint {checkpoint_id} contains manifest checkpoint {} while no durable Commit outcome exists",
                    manifest.checkpoint_id
                )));
            }
            if manifest.durable_phase == DurableCheckpointPhase::Finalized {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] finalized checkpoint {} epoch {} exists in recovery inventory but no durable Commit outcome exists",
                    manifest.checkpoint_id, manifest.epoch
                )));
            }
        }
        Ok(())
    }

    /// Recover from the latest committed, structurally valid checkpoint.
    ///
    /// Returns `Ok(None)` when no committed checkpoint exists. With an outcome store, Prepared
    /// artifacts without a Commit outcome are explicitly ignored as abandoned attempts.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the store fails or no stored checkpoint is usable.
    pub(crate) async fn recover(
        &self,
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<Option<RecoveredState>, DbError> {
        self.recover_with_authority(decision_store.map(RecoveryOutcomeAuthority::Local))
            .await
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn recover_cluster(
        &self,
        authority: &laminar_core::cluster::control::LeaderLeaseStore,
        capsule_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    ) -> Result<Option<RecoveredState>, DbError> {
        self.recover_with_authority(Some(RecoveryOutcomeAuthority::Cluster {
            outcomes: authority,
            capsules: capsule_store,
        }))
        .await
    }

    async fn recover_with_authority(
        &self,
        authority: Option<RecoveryOutcomeAuthority<'_>>,
    ) -> Result<Option<RecoveredState>, DbError> {
        self.validate_outcome_authority(authority)?;
        // A Commit outcome is the irrevocable recovery frontier. Once it exists, restoring any
        // older manifest can replay output already visible in an exact external sink. Resolve the
        // highest commit first and require an exact participant-bound manifest. Corruption or
        // storage loss for an included participant is fatal rather than a reason to rewind.
        if let Some(authority) = authority {
            let (outcomes, cluster_dominance) = match authority {
                RecoveryOutcomeAuthority::Local(decision_store) => {
                    let mut outcomes = authority.outcomes().await?;
                    // Prepared is the sole local pre-outcome durable phase: state is sealed and
                    // coordinated sinks may have phase-1 side effects, but Finalized is written
                    // only after Commit. Settle every outcome-less Prepared attempt before
                    // choosing an older recovery cut.
                    self.settle_local_prepared_attempts(decision_store, &outcomes)
                        .await?;
                    outcomes = authority.outcomes().await?;
                    (outcomes, None)
                }
                #[cfg(feature = "cluster")]
                RecoveryOutcomeAuthority::Cluster {
                    outcomes: cluster_authority,
                    capsules,
                } => {
                    let outcomes = self
                        .validated_cluster_recovery_outcomes(cluster_authority, capsules)
                        .await?;
                    let dominance = ClusterPreparedDominance::from_outcomes(&outcomes);
                    (outcomes, dominance)
                }
            };
            let outcome = outcomes
                .iter()
                .rev()
                .find(|outcome| outcome.is_commit())
                .cloned();
            if let Some(outcome) = outcome {
                self.validate_newer_durable_attempts(&outcome, &outcomes, cluster_dominance)
                    .await?;
                return self.restore_committed_outcome(&outcome, authority).await;
            }

            self.validate_no_commit_outcome_genesis().await?;
            info!(
                "no Commit outcome exists; ignoring aborted or unresolved Prepared artifacts and starting fresh"
            );
            return Ok(None);
        }
        let mut checkpoint_ids = self.store.list_ids().await.map_err(DbError::from)?;
        if checkpoint_ids.is_empty() {
            info!("checkpoint store is empty, starting fresh");
            return Ok(None);
        }
        checkpoint_ids.reverse();
        self.restore_first_for_participant(&checkpoint_ids, authority, self.store.participant_id())
            .await
    }

    /// Recover from the newest viable checkpoint with `epoch <= target_epoch` — the
    /// coordinated-restart target, which may be older than this node's local latest.
    ///
    /// # Errors
    /// Returns `DbError::Checkpoint` if the store fails.
    pub(crate) async fn recover_to_epoch(
        &self,
        target_epoch: u64,
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<Option<RecoveredState>, DbError> {
        self.recover_to_epoch_with_authority(
            target_epoch,
            decision_store.map(RecoveryOutcomeAuthority::Local),
        )
        .await
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn recover_cluster_to_epoch(
        &self,
        target_epoch: u64,
        authority: &laminar_core::cluster::control::LeaderLeaseStore,
        capsule_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    ) -> Result<Option<RecoveredState>, DbError> {
        self.recover_to_epoch_with_authority(
            target_epoch,
            Some(RecoveryOutcomeAuthority::Cluster {
                outcomes: authority,
                capsules: capsule_store,
            }),
        )
        .await
    }

    async fn recover_to_epoch_with_authority(
        &self,
        target_epoch: u64,
        authority: Option<RecoveryOutcomeAuthority<'_>>,
    ) -> Result<Option<RecoveredState>, DbError> {
        self.validate_outcome_authority(authority)?;
        if let Some(authority) = authority {
            // Audit the immutable outcome history before selecting a target. Abort closes an
            // attempt but never creates a recovery cut; the newest retained Commit is authoritative.
            let (outcomes, cluster_dominance) = match authority {
                RecoveryOutcomeAuthority::Local(decision_store) => {
                    let mut outcomes = authority.outcomes().await?;
                    self.settle_local_prepared_attempts(decision_store, &outcomes)
                        .await?;
                    outcomes = authority.outcomes().await?;
                    (outcomes, None)
                }
                #[cfg(feature = "cluster")]
                RecoveryOutcomeAuthority::Cluster {
                    outcomes: cluster_authority,
                    capsules,
                } => {
                    let outcomes = self
                        .validated_cluster_recovery_outcomes(cluster_authority, capsules)
                        .await?;
                    let dominance = ClusterPreparedDominance::from_outcomes(&outcomes);
                    (outcomes, dominance)
                }
            };
            let committed = outcomes
                .iter()
                .rev()
                .find(|outcome| outcome.is_commit())
                .cloned();
            return match committed {
                Some(outcome) if outcome.epoch == target_epoch => {
                    self.validate_newer_durable_attempts(
                        &outcome,
                        &outcomes,
                        cluster_dominance,
                    )
                        .await?;
                    self.restore_committed_outcome(&outcome, authority).await
                }
                Some(outcome) => Err(DbError::Checkpoint(format!(
                    "[LDB-6041] recovery target epoch {target_epoch} is not the highest durable Commit outcome: epoch {} checkpoint {} is authoritative",
                    outcome.epoch, outcome.checkpoint_id
                ))),
                None if target_epoch == 0 => {
                    self.validate_no_commit_outcome_genesis().await?;
                    Ok(None)
                }
                None => Err(DbError::Checkpoint(format!(
                    "[LDB-6041] recovery target epoch {target_epoch} has no Commit outcome"
                ))),
            };
        }

        let checkpoint_ids = self.store.list_ids().await.map_err(DbError::from)?;
        if checkpoint_ids.is_empty() {
            return if target_epoch == 0 {
                Ok(None)
            } else {
                Err(DbError::Checkpoint(format!(
                    "[LDB-6041] recovery target epoch {target_epoch} has no checkpoint history"
                )))
            };
        }

        let mut checkpoints = Vec::with_capacity(checkpoint_ids.len());
        for &id in &checkpoint_ids {
            match self.store.load_by_id(id).await {
                Ok(Some(manifest)) if manifest.checkpoint_id == id => {
                    if manifest.epoch <= target_epoch {
                        checkpoints.push((id, manifest.epoch));
                    }
                }
                Ok(Some(manifest)) => warn!(
                    storage_id = id,
                    manifest_id = manifest.checkpoint_id,
                    "checkpoint identity mismatch — skipping"
                ),
                Ok(None) => {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6041] checkpoint {id} disappeared during recovery inventory"
                    )));
                }
                Err(CheckpointStoreError::Serde(e)) => {
                    warn!(checkpoint_id = id, error = %e, "corrupt checkpoint manifest — skipping");
                }
                Err(e) => return Err(DbError::from(e)),
            }
        }
        // Newest eligible first. The globally unique checkpoint ID is the authoritative tie-break
        // and prevents abandoned attempts from winning through store iteration order.
        checkpoints.sort_by_key(|&(id, epoch)| std::cmp::Reverse((epoch, id)));
        if checkpoints.is_empty() {
            return if target_epoch == 0 {
                Ok(None)
            } else {
                Err(DbError::Checkpoint(format!(
                    "[LDB-6041] no usable checkpoint exists at or before recovery target \
                     epoch {target_epoch}"
                )))
            };
        }
        let candidate_ids: Vec<u64> = checkpoints.iter().map(|&(id, _)| id).collect();
        self.restore_first_for_participant(&candidate_ids, authority, self.store.participant_id())
            .await
    }

    fn validate_outcome_authority(
        &self,
        authority: Option<RecoveryOutcomeAuthority<'_>>,
    ) -> Result<(), DbError> {
        match authority {
            Some(authority) if authority.scope() != self.expected_outcome_scope => {
                Err(DbError::Checkpoint(format!(
                    "[LDB-6041] recovery authority scope {:?} does not match active runtime scope {:?}",
                    authority.scope(),
                    self.expected_outcome_scope
                )))
            }
            None if self.expected_outcome_scope == CheckpointScope::Cluster => {
                Err(DbError::Checkpoint(
                    "[LDB-6041] cluster recovery requires the exact checkpoint authority".into(),
                ))
            }
            _ => Ok(()),
        }
    }

    /// Resolve external operator states from the sidecar file into inline entries.
    ///
    /// # Errors
    /// Returns a checkpoint error if the sidecar is unavailable, truncated, or unreadable.
    fn resolve_external_states(
        manifest: &mut CheckpointManifest,
        state_data: Option<&[u8]>,
    ) -> Result<(), DbError> {
        let external_ops: Vec<String> = manifest
            .operator_states
            .iter()
            .filter(|(_, op)| op.external)
            .map(|(name, _)| name.clone())
            .collect();

        if external_ops.is_empty() {
            return Ok(());
        }

        let Some(state_data) = state_data else {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6010] checkpoint {} sidecar is missing for external operators \
                 {external_ops:?}",
                manifest.checkpoint_id
            )));
        };

        for (name, op) in &mut manifest.operator_states {
            if op.external {
                // Checked arithmetic: a corrupt manifest can't overflow the length check.
                let range = match (
                    usize::try_from(op.external_offset),
                    usize::try_from(op.external_length),
                ) {
                    (Ok(start), Ok(len)) => start.checked_add(len).map(|end| (start, end)),
                    _ => None,
                }
                .filter(|&(_, end)| end <= state_data.len());
                if let Some((start, end)) = range {
                    let external_offset = op.external_offset;
                    let external_length = op.external_length;
                    let data = &state_data[start..end];
                    *op = laminar_core::storage::checkpoint_manifest::OperatorCheckpoint::inline(
                        data,
                    );
                    debug!(
                        operator = %name,
                        offset = external_offset,
                        length = external_length,
                        "resolved external operator state from sidecar"
                    );
                } else {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6010] checkpoint {} sidecar range for operator '{name}' is invalid \
                         (offset {}, length {}, sidecar {})",
                        manifest.checkpoint_id,
                        op.external_offset,
                        op.external_length,
                        state_data.len()
                    )));
                }
            }
        }
        Ok(())
    }

    /// Inner restore logic shared by the fast path and the fallback loop.
    fn restore_from(artifacts: CheckpointArtifacts) -> Result<RecoveredState, DbError> {
        let CheckpointArtifacts {
            mut manifest,
            state_data,
        } = artifacts;
        Self::resolve_external_states(&mut manifest, state_data.as_deref())?;

        info!(
            checkpoint_id = manifest.checkpoint_id,
            epoch = manifest.epoch,
            "recovering from checkpoint"
        );
        Ok(RecoveredState {
            manifest,
            outcome: None,
            #[cfg(feature = "cluster")]
            cluster_capsule: None,
            #[cfg(feature = "cluster")]
            vnode_restore_cut: None,
        })
    }

    fn validate_outcome_manifest_binding(
        &self,
        outcome: &CheckpointOutcome,
        storage_id: u64,
        manifest: &CheckpointManifest,
        storage_participant: u64,
    ) -> Result<(), DbError> {
        if outcome.scope != self.expected_outcome_scope {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] durable outcome scope {:?} does not match active runtime scope {:?}",
                outcome.scope, self.expected_outcome_scope
            )));
        }
        if outcome.checkpoint_id != storage_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] epoch {} commits checkpoint {}, but storage candidate is {storage_id}",
                manifest.epoch, outcome.checkpoint_id
            )));
        }
        if outcome.deployment_id != manifest.deployment_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] epoch {} checkpoint {storage_id} outcome deployment '{}' does not match manifest deployment '{}'",
                manifest.epoch, outcome.deployment_id, manifest.deployment_id
            )));
        }
        let outcome_participants = outcome.assignment_fence.as_ref().map_or_else(
            || vec![0],
            laminar_core::checkpoint::CheckpointAssignmentFence::participant_ids,
        );
        if outcome_participants
            .binary_search(&storage_participant)
            .is_err()
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] epoch {} checkpoint {storage_id} storage participant {storage_participant} is absent from outcome participants {:?}",
                manifest.epoch, outcome_participants
            )));
        }
        if outcome.verdict != CheckpointVerdict::Commit {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] epoch {} checkpoint {storage_id} has an Abort outcome",
                manifest.epoch
            )));
        }
        match (outcome.scope, outcome.recovery_capsule.as_ref()) {
            (CheckpointScope::Local, None) | (CheckpointScope::Cluster, Some(_)) => {}
            (CheckpointScope::Local, Some(_)) => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] local epoch {} checkpoint {storage_id} unexpectedly binds a cluster recovery capsule",
                    manifest.epoch
                )));
            }
            (CheckpointScope::Cluster, None) => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] cluster epoch {} checkpoint {storage_id} has no recovery capsule",
                    manifest.epoch
                )));
            }
        }
        Ok(())
    }

    async fn complete_restore(
        &self,
        mut state: RecoveredState,
        outcome: Option<CheckpointOutcome>,
        storage_id: u64,
        storage_participant: u64,
    ) -> Result<RecoveredState, DbError> {
        state.outcome = outcome;
        if state.manifest.durable_phase == DurableCheckpointPhase::Prepared {
            if storage_participant == self.store.participant_id() {
                self.store
                    .finalize(storage_id)
                    .await
                    .map_err(DbError::from)?;
            }
            // A Commit outcome is authoritative. A donor manifest is not republished locally.
            state.manifest.durable_phase = DurableCheckpointPhase::Finalized;
        }
        Ok(state)
    }

    /// Restore from `artifacts` if viable; `None` means an older checkpoint may be tried
    /// because this candidate is deterministically corrupt or has no durable Commit outcome.
    async fn try_restore(
        &self,
        storage_id: u64,
        artifacts: CheckpointArtifacts,
        authority: Option<RecoveryOutcomeAuthority<'_>>,
        storage_participant: u64,
        known_outcome: Option<&CheckpointOutcome>,
    ) -> Result<Option<RecoveredState>, DbError> {
        let manifest = &artifacts.manifest;
        let (checkpoint_id, epoch) = (manifest.checkpoint_id, manifest.epoch);
        if manifest.participant_id != storage_participant {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] checkpoint {storage_id} manifest participant {} does not match \
                 storage participant {storage_participant}",
                manifest.participant_id
            )));
        }
        if manifest.pipeline_identity != self.expected_pipeline_identity {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] checkpoint {storage_id} pipeline identity {} does not match runtime \
                 identity {}; explicit checkpoint reset or savepoint migration is required",
                manifest.pipeline_identity.sha256, self.expected_pipeline_identity.sha256
            )));
        }
        if manifest.deployment_id != self.expected_deployment_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] checkpoint {storage_id} deployment identity '{}' does not match \
                 runtime identity '{}'; a partial storage reset is unsafe",
                manifest.deployment_id, self.expected_deployment_id
            )));
        }
        let outcome = match known_outcome {
            Some(outcome) => Some(outcome.clone()),
            None => match authority {
                Some(authority) => authority
                    .outcome(epoch)
                    .await?
                    .filter(CheckpointOutcome::is_commit),
                None => None,
            },
        };
        if let Some(ref outcome) = outcome {
            self.validate_outcome_manifest_binding(
                outcome,
                storage_id,
                manifest,
                storage_participant,
            )?;
        }

        // Prepared inventory is never a recovery cut on its own. When an exact outcome store is
        // configured, it is authoritative for Finalized manifests too: the manifest phase is a
        // publication optimization, not a second commit oracle. At-least-once runtimes without a
        // outcome store may recover only an integrity-valid Finalized manifest.
        if outcome.is_none()
            && (manifest.durable_phase == DurableCheckpointPhase::Prepared || authority.is_some())
        {
            warn!(
                checkpoint_id,
                epoch,
                phase = ?manifest.durable_phase,
                "checkpoint has no exact Commit outcome; trying older"
            );
            return Ok(None);
        }

        let (artifacts, validation) = artifacts
            .validate(
                storage_id,
                storage_participant,
                self.store.key_group_count(),
                self.store.max_state_data_bytes(),
            )
            .await
            .map_err(DbError::from)?;
        if !validation.valid {
            error!(
                checkpoint_id,
                issues = ?validation.issues,
                "[LDB-6010] checkpoint integrity check failed"
            );
            warn!(
                checkpoint_id,
                epoch, "[LDB-6010] checkpoint corrupt, trying older"
            );
            if outcome.is_some() {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] committed checkpoint {storage_id} is corrupt"
                )));
            }
            return Ok(None);
        }
        let state = Self::restore_from(artifacts)?;
        Ok(Some(
            self.complete_restore(state, outcome, storage_id, storage_participant)
                .await?,
        ))
    }

    /// Restore from the first viable checkpoint ID in try order.
    async fn restore_first_for_participant(
        &self,
        candidates: &[u64],
        authority: Option<RecoveryOutcomeAuthority<'_>>,
        storage_participant: u64,
    ) -> Result<Option<RecoveredState>, DbError> {
        for &checkpoint_id in candidates {
            match self
                .store
                .load_checkpoint_artifacts_for_participant(storage_participant, checkpoint_id)
                .await
            {
                Ok(Some(artifacts)) => {
                    if let Some(state) = self
                        .try_restore(
                            checkpoint_id,
                            artifacts,
                            authority,
                            storage_participant,
                            None,
                        )
                        .await?
                    {
                        return Ok(Some(state));
                    }
                }
                Ok(None) => {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6041] checkpoint {checkpoint_id} is absent from participant \
                         {storage_participant} recovery inventory"
                    )));
                }
                Err(CheckpointStoreError::Serde(e)) => {
                    warn!(checkpoint_id, error = %e, "corrupt checkpoint manifest, trying older");
                }
                Err(e) => return Err(DbError::from(e)),
            }
        }
        Err(Self::no_usable_checkpoint_error(candidates))
    }

    fn no_usable_checkpoint_error(checkpoint_ids: &[u64]) -> DbError {
        DbError::Checkpoint(format!(
            "[LDB-6041] checkpoint history exists but none is usable; \
             refusing to start with empty state (checkpoint ids: {checkpoint_ids:?})"
        ))
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod rehydration_tests;
#[cfg(test)]
mod restore_input_resource_tests;
