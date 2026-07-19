//! Checkpoint recovery: selects a committed manifest and resolves its operator-state sidecar.
//! Runtime owners restore sources, sinks, tables, and operators from that single recovered cut.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::ClusterRecoveryCapsule;
use laminar_core::checkpoint_decision::{CheckpointOutcome, CheckpointScope, CheckpointVerdict};
use laminar_core::state::{CheckpointAttempt, CheckpointSealInventory, StateBackend};
#[cfg(feature = "cluster")]
use laminar_core::storage::checkpoint_manifest::ConnectorCheckpoint;
use laminar_core::storage::checkpoint_manifest::{
    CheckpointManifest, DurableCheckpointPhase, PipelineIdentity,
};
use laminar_core::storage::checkpoint_store::{
    CheckpointArtifacts, CheckpointStore, CheckpointStoreError,
};
use sha2::{Digest, Sha256};
use tracing::{debug, error, info, warn};

use crate::error::DbError;

const VNODE_REHYDRATION_CONCURRENCY: usize = 32;

/// Result of a successful recovery from a checkpoint.
#[derive(Debug)]
pub struct RecoveredState {
    /// Manifest that was restored from.
    pub manifest: CheckpointManifest,
    outcome: Option<CheckpointOutcome>,
    #[cfg(feature = "cluster")]
    cluster_capsule: Option<ClusterRecoveryCapsule>,
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
}

/// Outcome of rehydrating a set of vnodes from durable state.
#[derive(Debug, Default)]
pub struct VnodeRehydration {
    /// Exact sealed attempt the partials were read from. `None` only for an empty request.
    pub attempt: Option<CheckpointAttempt>,
    /// vnode → recovery chain (oldest→newest): a FULL base followed by any delta partials.
    pub restored: HashMap<u32, Vec<Bytes>>,
}

impl VnodeRehydration {
    /// Number of vnodes successfully read back.
    #[must_use]
    pub fn restored_count(&self) -> usize {
        self.restored.len()
    }
}

/// Reads an exact Commit-outcome-bound `partial.bin` chain for requested vnodes.
/// Applying the bytes is the caller's responsibility.
pub struct VnodeRehydrator<'a> {
    backend: &'a dyn StateBackend,
    seal_cache: tokio::sync::Mutex<HashMap<CheckpointAttempt, Arc<CheckpointSealInventory>>>,
}

impl<'a> VnodeRehydrator<'a> {
    /// Create a rehydrator over `backend`.
    #[must_use]
    pub fn new(backend: &'a dyn StateBackend) -> Self {
        Self {
            backend,
            seal_cache: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn sealed_inventory(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Arc<CheckpointSealInventory>, DbError> {
        if let Some(inventory) = self.seal_cache.lock().await.get(&attempt).cloned() {
            return Ok(inventory);
        }
        let inventory = self
            .backend
            .checkpoint_seal_inventory(attempt)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] failed to read exact state seal for checkpoint {} epoch {}: {error}",
                    attempt.checkpoint_id, attempt.epoch
                ))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] checkpoint {} epoch {} has no exact state seal",
                    attempt.checkpoint_id, attempt.epoch
                ))
            })?;
        if inventory.attempt != attempt {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] requested state attempt {attempt:?} does not match seal inventory attempt {:?}",
                inventory.attempt
            )));
        }
        let partial_vnodes: Vec<u32> = inventory
            .sealed_partials
            .iter()
            .map(|partial| partial.vnode)
            .collect();
        if inventory
            .required_vnodes
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
            || partial_vnodes != inventory.required_vnodes
            || inventory
                .sealed_partials
                .iter()
                .any(|partial| partial.assignment_version != inventory.assignment_version)
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] checkpoint {} epoch {} has a non-canonical sealed vnode inventory",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        let inventory = Arc::new(inventory);
        self.seal_cache
            .lock()
            .await
            .insert(attempt, Arc::clone(&inventory));
        Ok(inventory)
    }

    async fn read_verified_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Bytes, DbError> {
        let inventory = self.sealed_inventory(attempt).await?;
        let index = inventory
            .sealed_partials
            .binary_search_by_key(&vnode, |partial| partial.vnode)
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] vnode {vnode} is absent from the exact state seal for checkpoint {} epoch {}",
                    attempt.checkpoint_id, attempt.epoch
                ))
            })?;
        let attestation = &inventory.sealed_partials[index];
        let bytes = self
            .backend
            .read_partial(attempt, vnode)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] failed to read vnode {vnode} at sealed epoch {} checkpoint {}: {error}",
                    attempt.epoch, attempt.checkpoint_id
                ))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} is missing at sealed epoch {} checkpoint {}",
                    attempt.epoch, attempt.checkpoint_id
                ))
            })?;
        let actual_digest = format!("{:x}", Sha256::digest(&bytes));
        if u64::try_from(bytes.len()).ok() != Some(attestation.payload_len)
            || actual_digest != attestation.payload_sha256
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] vnode {vnode} payload does not match the exact state seal for checkpoint {} epoch {}",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        Ok(bytes)
    }

    /// Read each vnode's partial chain pinned at `epoch` (a committed cut chosen by the caller),
    /// so boot recovery restores state at the same epoch its source offsets resume from.
    ///
    /// # Errors
    /// Returns a checkpoint error unless every requested vnode has a complete, decodable chain.
    pub async fn rehydrate_at(
        &self,
        vnodes: &[u32],
        attempt: CheckpointAttempt,
    ) -> Result<VnodeRehydration, DbError> {
        let mut report = VnodeRehydration::default();
        if vnodes.is_empty() {
            return Ok(report);
        }
        let inventory = self.sealed_inventory(attempt).await?;
        if let Some(unsealed) = vnodes
            .iter()
            .find(|vnode| inventory.required_vnodes.binary_search(vnode).is_err())
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] vnode {unsealed} is absent from the exact state seal for checkpoint {} epoch {}",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        report.attempt = Some(attempt);

        let chains = futures::stream::iter(vnodes.iter().copied().map(|vnode| async move {
            let chain = self.collect_chain(vnode, attempt).await?;
            Ok::<_, DbError>((vnode, chain))
        }))
        .buffer_unordered(VNODE_REHYDRATION_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
        for (vnode, chain) in chains {
            debug!(
                vnode,
                epoch = attempt.epoch,
                checkpoint_id = attempt.checkpoint_id,
                links = chain.len(),
                "rehydrated vnode chain"
            );
            report.restored.insert(vnode, chain);
        }

        info!(
            epoch = attempt.epoch,
            checkpoint_id = attempt.checkpoint_id,
            restored = report.restored.len(),
            "vnode rehydration complete"
        );
        Ok(report)
    }

    /// Resolve a vnode's recovery chain at an exact attempt: collapse leading reference hops,
    /// then walk exact parent attempts until each head op has its FULL base (oldest→newest).
    async fn collect_chain(
        &self,
        vnode: u32,
        attempt: CheckpointAttempt,
    ) -> Result<Vec<Bytes>, DbError> {
        use crate::vnode_partial::VnodePartial;

        let (bytes, mut current, head) = self.resolve_reference_head(vnode, attempt).await?;
        // Walk back until each delta operator has its FULL base.
        let mut need: std::collections::HashSet<String> = head
            .deltas
            .iter()
            .map(|(n, _)| n.clone())
            .filter(|n| !head.operators.iter().any(|(on, _)| on == n))
            .collect();
        let mut chain_rev: Vec<Bytes> = vec![bytes];
        let mut cur = head;
        while !need.is_empty() {
            let Some(parent) = cur.base else {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} delta chain has no FULL base for operators {need:?}"
                )));
            };
            if parent.epoch >= current.epoch || parent.checkpoint_id >= current.checkpoint_id {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} has non-decreasing delta link \
                     {current:?}->{parent:?}"
                )));
            }
            let pbytes = self.read_verified_partial(parent, vnode).await.map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} delta parent {parent:?} failed seal verification: {error}"
                ))
            })?;
            let pp = VnodePartial::decode(&pbytes).map_err(|e| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} delta parent {parent:?} is invalid: {e}"
                ))
            })?;
            for (n, _) in &pp.operators {
                need.remove(n);
            }
            chain_rev.push(pbytes);
            current = parent;
            cur = pp;
        }
        chain_rev.reverse();
        Ok(chain_rev)
    }

    /// Collapse reference-only partials and return the first FULL or DELTA head without decoding
    /// that head twice on the recovery path.
    async fn resolve_reference_head(
        &self,
        vnode: u32,
        attempt: CheckpointAttempt,
    ) -> Result<(Bytes, CheckpointAttempt, crate::vnode_partial::VnodePartial), DbError> {
        use crate::vnode_partial::VnodePartial;

        let mut bytes = self.read_verified_partial(attempt, vnode).await?;

        let mut current = attempt;
        loop {
            let partial = VnodePartial::decode(&bytes).map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} has an invalid partial at epoch {} checkpoint {}: \
                     {error}",
                    current.epoch, current.checkpoint_id
                ))
            })?;
            if !partial.operators.is_empty() || !partial.deltas.is_empty() {
                return Ok((bytes, current, partial));
            }
            let Some(base) = partial.base else {
                return Ok((bytes, current, partial));
            };
            if base.epoch >= current.epoch || base.checkpoint_id >= current.checkpoint_id {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} has non-decreasing reference \
                     {current:?}->{base:?}"
                )));
            }
            bytes = self.read_verified_partial(base, vnode).await.map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} reference base {base:?} failed seal verification: {error}"
                ))
            })?;
            current = base;
        }
    }
}

/// One operator's resolved recovery chain: FULL base bytes + ordered changed-state deltas.
#[cfg(feature = "cluster")]
pub(crate) type ResolvedOpChain<'a> = (&'a [u8], Vec<&'a [u8]>);

/// From a vnode's recovery chain (oldest→newest decoded partials), resolve one operator's FULL base
/// bytes + ordered delta payloads. Returns `None` when no FULL for `op` is present (start fresh).
#[cfg(feature = "cluster")]
#[must_use]
pub(crate) fn resolve_op_chain<'a>(
    chain: &'a [crate::vnode_partial::VnodePartial],
    op: &str,
) -> Option<ResolvedOpChain<'a>> {
    let base_idx = chain
        .iter()
        .rposition(|p| p.operators.iter().any(|(n, _)| n == op))?;
    let base = chain[base_idx]
        .operators
        .iter()
        .find(|(n, _)| n == op)
        .map(|(_, b)| b.as_slice())?;
    let mut deltas = Vec::new();
    for p in &chain[base_idx + 1..] {
        if let Some((_, d)) = p.deltas.iter().find(|(n, _)| n == op) {
            deltas.push(d.as_slice());
        }
    }
    Some((base, deltas))
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

impl<'a> RecoveryOutcomeAuthority<'a> {
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
                .cluster_outcomes()
                .await
                .map_err(|error| DbError::Checkpoint(error.to_string())),
        }
    }

    #[cfg(feature = "cluster")]
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
            let validation = artifacts.validate(
                outcome.checkpoint_id,
                storage_participant,
                self.store.key_group_count(),
            );
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

            let mut recovered = match self.restore_from(artifacts) {
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
    async fn validated_cluster_prepared_dominance(
        &self,
        authority: &laminar_core::cluster::control::LeaderLeaseStore,
        capsule_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    ) -> Result<(), DbError> {
        authority
            .validated_cluster_outcome_retention_boundary(|outcome| async move {
                self.preflight_cluster_committed_outcome(&outcome, capsule_store)
                    .await
                    .map(drop)
                    .map_err(|error| error.to_string())
            })
            .await
            .map(drop)
            .map_err(|error| DbError::Checkpoint(error.to_string()))
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
                    self.validated_cluster_prepared_dominance(cluster_authority, capsules)
                        .await?;
                    let outcomes = authority.outcomes().await?;
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
                    self.validated_cluster_prepared_dominance(cluster_authority, capsules)
                        .await?;
                    let outcomes = authority.outcomes().await?;
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

        let state_data = match state_data {
            Some(data) => data,
            None => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6010] checkpoint {} sidecar is missing for external operators \
                     {external_ops:?}",
                    manifest.checkpoint_id
                )));
            }
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
    fn restore_from(&self, artifacts: CheckpointArtifacts) -> Result<RecoveredState, DbError> {
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
        })
    }

    /// Returns `true` if the checkpoint fails integrity validation.
    ///
    /// Operational validation failures propagate instead of being mislabeled as deterministic
    /// corruption and silently falling back while durable storage is unavailable.
    fn is_checkpoint_corrupt(
        &self,
        storage_id: u64,
        artifacts: &CheckpointArtifacts,
        storage_participant: u64,
    ) -> bool {
        let manifest = &artifacts.manifest;
        if manifest.checkpoint_id != storage_id {
            error!(
                storage_id,
                manifest_id = manifest.checkpoint_id,
                "[LDB-6010] checkpoint identity mismatch"
            );
            return true;
        }
        let validation = artifacts.validate(
            storage_id,
            storage_participant,
            self.store.key_group_count(),
        );
        if !validation.valid {
            error!(
                checkpoint_id = manifest.checkpoint_id,
                issues = ?validation.issues,
                "[LDB-6010] checkpoint integrity check failed"
            );
        }
        !validation.valid
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

        if self.is_checkpoint_corrupt(storage_id, &artifacts, storage_participant) {
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
        let state = self.restore_from(artifacts)?;
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
mod tests {
    use super::*;
    use laminar_core::storage::checkpoint_manifest::OperatorCheckpoint;
    use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;
    #[cfg(feature = "cluster")]
    use laminar_core::storage::checkpoint_store::ObjectStoreCheckpointStore;

    fn make_store(dir: &std::path::Path) -> FileSystemCheckpointStore {
        FileSystemCheckpointStore::new(dir)
    }

    fn finalized_manifest(id: u64, epoch: u64) -> CheckpointManifest {
        let mut manifest = CheckpointManifest::new(id, epoch);
        manifest.durable_phase = DurableCheckpointPhase::Finalized;
        manifest
    }

    fn pipeline_identity(byte: u8) -> PipelineIdentity {
        PipelineIdentity {
            canonical_version:
                laminar_core::storage::checkpoint_manifest::PIPELINE_IDENTITY_VERSION,
            sha256: format!("{byte:02x}").repeat(32),
        }
    }

    #[cfg(feature = "cluster")]
    fn assignment_fence(
        version: u64,
        participants: &[u64],
    ) -> laminar_core::checkpoint::CheckpointAssignmentFence {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

        let participants = participants
            .iter()
            .map(|node_id| CheckpointParticipant {
                node_id: *node_id,
                boot_incarnation: format!("00000000-0000-0000-0000-{node_id:012x}")
                    .parse()
                    .unwrap(),
            })
            .collect::<Vec<_>>();
        let owners = participants
            .iter()
            .map(|participant| participant.node_id)
            .collect::<Vec<_>>();
        CheckpointAssignmentFence::from_owner_map(version, &owners, participants).unwrap()
    }

    #[cfg(feature = "cluster")]
    struct ClusterDecisions {
        capsules: laminar_core::checkpoint_decision::CheckpointDecisionStore,
        authority: laminar_core::cluster::control::LeaderLeaseStore,
        proof: laminar_core::checkpoint::LeaderProof,
    }

    #[cfg(feature = "cluster")]
    impl std::ops::Deref for ClusterDecisions {
        type Target = laminar_core::checkpoint_decision::CheckpointDecisionStore;

        fn deref(&self) -> &Self::Target {
            &self.capsules
        }
    }

    #[cfg(feature = "cluster")]
    async fn cluster_decisions(
        backing: std::sync::Arc<dyn object_store::ObjectStore>,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        leader_id: u64,
    ) -> ClusterDecisions {
        use laminar_core::cluster::control::{LeaderLeaseOwner, LeaseOutcome};

        let capsules = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::clone(&backing),
        );
        capsules.load_or_create_deployment_id().await.unwrap();
        let authority = laminar_core::cluster::control::LeaderLeaseStore::new(backing, 60_000);
        let owner = LeaderLeaseOwner {
            node: laminar_core::cluster::discovery::NodeId(leader_id),
            boot: fence
                .participant_incarnation(leader_id)
                .expect("test leader belongs to the assignment certificate"),
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            unreachable!("fresh cluster authority must grant its first lease")
        };
        ClusterDecisions {
            capsules,
            authority,
            proof: lease.proof(),
        }
    }

    async fn record_local_commit(
        store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
        epoch: u64,
        checkpoint_id: u64,
    ) {
        store
            .record_outcome(
                epoch,
                checkpoint_id,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .unwrap();
    }

    async fn record_local_abort(
        store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
        epoch: u64,
        checkpoint_id: u64,
    ) {
        store
            .record_outcome(
                epoch,
                checkpoint_id,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
    }

    #[cfg(feature = "cluster")]
    async fn record_cluster_capsule_commit(
        store: &ClusterDecisions,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        capsule: &laminar_core::checkpoint::ClusterRecoveryCapsule,
    ) {
        let capsule_ref = store.create_recovery_capsule(capsule).await.unwrap();
        store
            .authority
            .record_cluster_outcome(
                &store.proof,
                capsule.attempt.epoch,
                capsule.attempt.checkpoint_id,
                fence.clone(),
                CheckpointVerdict::Commit,
                Some(capsule_ref),
            )
            .await
            .unwrap();
    }

    #[cfg(feature = "cluster")]
    async fn record_cluster_commit(
        store: &ClusterDecisions,
        epoch: u64,
        checkpoint_id: u64,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        manifest_participant_id: u64,
        manifest: Option<&CheckpointManifest>,
    ) {
        let deployment_id = store.load_or_create_deployment_id().await.unwrap();
        let mut synthetic_manifest = CheckpointManifest::new(checkpoint_id, epoch);
        synthetic_manifest.participant_id = manifest_participant_id;
        synthetic_manifest.deployment_id.clone_from(&deployment_id);
        let manifest = manifest.unwrap_or(&synthetic_manifest);
        #[cfg(feature = "cluster")]
        let (manifest_sha256, portable_state_sha256) =
            crate::cluster_recovery_capsule::manifest_digests(manifest).unwrap();
        #[cfg(not(feature = "cluster"))]
        let (manifest_sha256, portable_state_sha256) = ("11".repeat(32), "22".repeat(32));
        let source_offsets = manifest
            .source_offsets
            .iter()
            .map(|(source, checkpoint)| {
                (
                    source.clone(),
                    checkpoint.offsets.clone().into_iter().collect(),
                )
            })
            .collect();
        let source_metadata = manifest
            .source_offsets
            .iter()
            .map(|(source, checkpoint)| {
                (
                    source.clone(),
                    checkpoint.metadata.clone().into_iter().collect(),
                )
            })
            .collect();
        let source_assignment_versions = manifest
            .source_offsets
            .iter()
            .filter_map(|(source, checkpoint)| {
                checkpoint
                    .source_assignment_version
                    .map(|version| (source.clone(), version))
            })
            .collect();
        let source_watermarks = manifest
            .source_watermarks
            .iter()
            .filter(|(source, _)| manifest.source_offsets.contains_key(*source))
            .map(|(source, watermark)| (source.clone(), *watermark))
            .collect();
        let participants = fence
            .participant_ids()
            .into_iter()
            .map(
                |participant_id| laminar_core::checkpoint::ParticipantRecoveryRef {
                    participant_id,
                    readiness_sha256: format!("{:x}", Sha256::digest(participant_id.to_le_bytes())),
                    manifest_sha256: manifest_sha256.clone(),
                    portable_state_sha256: portable_state_sha256.clone(),
                },
            )
            .collect();
        let capsule = laminar_core::checkpoint::ClusterRecoveryCapsule {
            version: laminar_core::checkpoint::CLUSTER_RECOVERY_CAPSULE_VERSION,
            attempt: CheckpointAttempt::new(epoch, checkpoint_id),
            deployment_id,
            pipeline_identity: manifest.pipeline_identity.clone(),
            assignment_fence: fence.clone(),
            seal_inventory_sha256: "33".repeat(32),
            participants,
            source_offsets,
            source_metadata,
            source_assignment_versions,
            source_watermarks,
            cluster_watermark: manifest.watermark.map_or(
                laminar_core::checkpoint::CheckpointWatermark::Uninitialized,
                laminar_core::checkpoint::CheckpointWatermark::Active,
            ),
            recovery_watermark_frontier: manifest.watermark,
            portable_state_sha256,
        };
        record_cluster_capsule_commit(store, fence, &capsule).await;
    }

    #[cfg(feature = "cluster")]
    async fn record_cluster_abort(
        store: &ClusterDecisions,
        epoch: u64,
        checkpoint_id: u64,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    ) {
        store
            .authority
            .record_cluster_outcome(
                &store.proof,
                epoch,
                checkpoint_id,
                fence.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
    }

    #[cfg(feature = "cluster")]
    async fn record_cluster_commit_for_manifests(
        store: &ClusterDecisions,
        epoch: u64,
        checkpoint_id: u64,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        manifests: &[(u64, &CheckpointManifest)],
    ) {
        assert_eq!(
            manifests.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            fence.participant_ids()
        );
        let source_manifest = manifests.first().unwrap().1;
        let mut portable_state_sha256 = None;
        let participants = manifests
            .iter()
            .map(|(participant_id, manifest)| {
                assert_eq!(manifest.participant_id, *participant_id);
                assert_eq!(manifest.epoch, epoch);
                assert_eq!(manifest.checkpoint_id, checkpoint_id);
                let (manifest_sha256, portable) =
                    crate::cluster_recovery_capsule::manifest_digests(manifest).unwrap();
                if let Some(expected) = portable_state_sha256.as_ref() {
                    assert_eq!(expected, &portable);
                } else {
                    portable_state_sha256 = Some(portable.clone());
                }
                laminar_core::checkpoint::ParticipantRecoveryRef {
                    participant_id: *participant_id,
                    readiness_sha256: format!("{:x}", Sha256::digest(participant_id.to_le_bytes())),
                    manifest_sha256,
                    portable_state_sha256: portable,
                }
            })
            .collect();
        let source_offsets = source_manifest
            .source_offsets
            .iter()
            .map(|(source, checkpoint)| {
                (
                    source.clone(),
                    checkpoint.offsets.clone().into_iter().collect(),
                )
            })
            .collect();
        let source_metadata = source_manifest
            .source_offsets
            .iter()
            .map(|(source, checkpoint)| {
                (
                    source.clone(),
                    checkpoint.metadata.clone().into_iter().collect(),
                )
            })
            .collect();
        let source_assignment_versions = source_manifest
            .source_offsets
            .iter()
            .filter_map(|(source, checkpoint)| {
                checkpoint
                    .source_assignment_version
                    .map(|version| (source.clone(), version))
            })
            .collect();
        let source_watermarks = source_manifest
            .source_watermarks
            .iter()
            .filter(|(source, _)| source_manifest.source_offsets.contains_key(*source))
            .map(|(source, watermark)| (source.clone(), *watermark))
            .collect();
        let capsule = laminar_core::checkpoint::ClusterRecoveryCapsule {
            version: laminar_core::checkpoint::CLUSTER_RECOVERY_CAPSULE_VERSION,
            attempt: CheckpointAttempt::new(epoch, checkpoint_id),
            deployment_id: source_manifest.deployment_id.clone(),
            pipeline_identity: source_manifest.pipeline_identity.clone(),
            assignment_fence: fence.clone(),
            seal_inventory_sha256: "33".repeat(32),
            participants,
            source_offsets,
            source_metadata,
            source_assignment_versions,
            source_watermarks,
            cluster_watermark: source_manifest.watermark.map_or(
                laminar_core::checkpoint::CheckpointWatermark::Uninitialized,
                laminar_core::checkpoint::CheckpointWatermark::Active,
            ),
            recovery_watermark_frontier: source_manifest.watermark,
            portable_state_sha256: portable_state_sha256.unwrap(),
        };
        record_cluster_capsule_commit(store, fence, &capsule).await;
    }

    #[tokio::test]
    async fn test_recover_no_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let mgr = RecoveryManager::new(&store);

        let result = mgr.recover(None).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_recover_empty_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Save a basic checkpoint
        let manifest = finalized_manifest(1, 5);
        store.save_with_state(&manifest, None).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(None).await.unwrap().unwrap();

        assert_eq!(result.epoch(), 5);
    }

    #[tokio::test]
    async fn recover_to_epoch_picks_newest_at_or_below_target() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        for (id, epoch) in [(1u64, 3u64), (2, 5), (3, 7)] {
            store.save(&finalized_manifest(id, epoch)).await.unwrap();
        }
        let mgr = RecoveryManager::new(&store);

        assert_eq!(
            mgr.recover_to_epoch(7, None)
                .await
                .unwrap()
                .unwrap()
                .epoch(),
            7
        );
        // A newer local epoch is rewound to the cluster-agreed target.
        assert_eq!(
            mgr.recover_to_epoch(6, None)
                .await
                .unwrap()
                .unwrap()
                .epoch(),
            5
        );
        assert_eq!(
            mgr.recover_to_epoch(5, None)
                .await
                .unwrap()
                .unwrap()
                .epoch(),
            5
        );
        // Only an explicit genesis rewind may start without a checkpoint.
        assert!(mgr.recover_to_epoch(0, None).await.unwrap().is_none());
        assert!(mgr.recover_to_epoch(2, None).await.is_err());
    }

    #[tokio::test]
    async fn recover_to_epoch_rejects_target_older_than_highest_commit_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        record_local_commit(&decisions, 5, 1).await;
        record_local_commit(&decisions, 7, 2).await;

        let error = RecoveryManager::new(&store)
            .recover_to_epoch(5, Some(&decisions))
            .await
            .expect_err("an older target must not rewind the durable Commit frontier");

        assert!(
            error.to_string().contains(
                "recovery target epoch 5 is not the highest durable Commit outcome: epoch 7 checkpoint 2 is authoritative"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn recover_to_genesis_rejects_finalized_inventory_without_latest_pointer() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        store.save(&finalized_manifest(1, 1)).await.unwrap();
        std::fs::remove_file(dir.path().join("checkpoints/latest.txt")).unwrap();
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );

        let error = RecoveryManager::new(&store)
            .recover_to_epoch(0, Some(&decisions))
            .await
            .expect_err("finalized inventory cannot be discarded as genesis");

        assert!(
            error
                .to_string()
                .contains("finalized checkpoint 1 epoch 1 exists in recovery inventory"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn recover_to_genesis_rejects_dangling_latest_pointer() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        store.save(&CheckpointManifest::new(1, 1)).await.unwrap();
        std::fs::write(
            dir.path().join("checkpoints/latest.txt"),
            "checkpoint_000099",
        )
        .unwrap();
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );

        let error = RecoveryManager::new(&store)
            .recover_to_epoch(0, Some(&decisions))
            .await
            .expect_err("a dangling recovery pointer cannot be treated as genesis");

        assert!(
            error.to_string().contains(
                "checkpoint recovery pointer is invalid while no durable Commit outcome exists"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn recover_to_genesis_allows_prepared_only_inventory() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        store.save(&CheckpointManifest::new(1, 1)).await.unwrap();
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );

        let recovered = RecoveryManager::new(&store)
            .recover_to_epoch(0, Some(&decisions))
            .await
            .unwrap();

        assert!(recovered.is_none());
        assert_eq!(
            store.load_by_id(1).await.unwrap().unwrap().durable_phase,
            DurableCheckpointPhase::Prepared
        );
    }

    #[tokio::test]
    async fn test_recover_with_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = finalized_manifest(1, 3);
        manifest.watermark = Some(42_000);
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(None).await.unwrap().unwrap();

        assert_eq!(result.manifest.watermark, Some(42_000));
    }

    #[tokio::test]
    async fn test_recover_with_operator_states() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = finalized_manifest(1, 7);
        manifest
            .operator_states
            .insert("0".to_string(), OperatorCheckpoint::inline(b"window-state"));
        manifest
            .operator_states
            .insert("3".to_string(), OperatorCheckpoint::inline(b"filter-state"));
        store.save_with_state(&manifest, None).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(None).await.unwrap().unwrap();

        assert_eq!(result.manifest.operator_states.len(), 2);
        let op0 = result.manifest.operator_states.get("0").unwrap();
        assert_eq!(op0.decode_inline().unwrap(), b"window-state");
    }

    #[tokio::test]
    async fn test_recover_table_store_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = finalized_manifest(1, 1);
        manifest.table_store_checkpoint_path = Some("/data/table_store_cp_001".into());
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(None).await.unwrap().unwrap();

        assert_eq!(
            result.manifest.table_store_checkpoint_path.as_deref(),
            Some("/data/table_store_cp_001")
        );
    }

    #[tokio::test]
    async fn test_recover_fallback_to_previous_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

        // Save two valid checkpoints
        let mut m1 = finalized_manifest(1, 10);
        m1.watermark = Some(1000);
        store.save(&m1).await.unwrap();

        let mut m2 = finalized_manifest(2, 20);
        m2.watermark = Some(2000);
        store.save(&m2).await.unwrap();

        // Corrupt the latest checkpoint by writing invalid JSON
        let latest_manifest_path = dir
            .path()
            .join("checkpoints")
            .join("checkpoint_000002")
            .join("manifest.json");
        std::fs::write(&latest_manifest_path, "not valid json!!!").unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(None).await.unwrap();

        // Should fall back to checkpoint 1
        let recovered = result.expect("should recover from fallback checkpoint");
        assert_eq!(recovered.manifest.checkpoint_id, 1);
        assert_eq!(recovered.epoch(), 10);
        assert_eq!(recovered.manifest.watermark, Some(1000));
    }

    #[tokio::test]
    async fn irrevocable_highest_commit_never_falls_back_to_older_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut older = finalized_manifest(1, 10);
        older.deployment_id.clone_from(&deployment_id);
        store.save(&older).await.unwrap();
        let mut committed = finalized_manifest(2, 20);
        committed.deployment_id.clone_from(&deployment_id);
        store.save(&committed).await.unwrap();
        record_local_commit(&decisions, 20, 2).await;
        let committed_manifest = dir
            .path()
            .join("checkpoints")
            .join("checkpoint_000002")
            .join("manifest.json");
        std::fs::write(committed_manifest, "corrupt").unwrap();

        let error = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .recover(Some(&decisions))
            .await
            .expect_err("a committed checkpoint cannot rewind to checkpoint 1");
        assert!(
            error.to_string().contains(
                "[LDB-6041] committed epoch 20 checkpoint 2 participant 0 artifacts are unreadable"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_recover_all_checkpoints_corrupt_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

        // Save a checkpoint then corrupt it
        store.save(&finalized_manifest(1, 5)).await.unwrap();

        let manifest_path = dir
            .path()
            .join("checkpoints")
            .join("checkpoint_000001")
            .join("manifest.json");
        std::fs::write(&manifest_path, "corrupt").unwrap();

        let mgr = RecoveryManager::new(&store);
        let error = mgr.recover(None).await.unwrap_err();
        assert!(error.to_string().contains("checkpoint history exists"));
    }

    #[tokio::test]
    async fn test_recover_latest_ok_no_fallback_needed() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

        store.save(&finalized_manifest(1, 10)).await.unwrap();
        store.save(&finalized_manifest(2, 20)).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(None).await.unwrap().unwrap();

        // Should use the latest (no fallback needed)
        assert_eq!(result.manifest.checkpoint_id, 2);
        assert_eq!(result.epoch(), 20);
    }

    #[tokio::test]
    async fn test_recover_with_sidecar_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Build a manifest with an external operator state
        let mut manifest = finalized_manifest(1, 5);
        let large_data = vec![0xAB; 2048];
        manifest
            .operator_states
            .insert("big-op".into(), OperatorCheckpoint::external(0, 2048));

        store
            .save_with_state(
                &manifest,
                Some(&[bytes::Bytes::copy_from_slice(&large_data)]),
            )
            .await
            .unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(None).await.unwrap().unwrap();

        // External state should have been resolved to inline
        let op = result.manifest.operator_states.get("big-op").unwrap();
        assert!(!op.external, "external state should be resolved to inline");
        assert_eq!(op.decode_inline().unwrap(), large_data);
    }

    #[tokio::test]
    async fn test_recover_mixed_inline_and_external() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = finalized_manifest(1, 3);
        // Small inline state
        manifest
            .operator_states
            .insert("small-op".into(), OperatorCheckpoint::inline(b"tiny"));
        // Large external state
        let large_data = vec![0xCD; 4096];
        manifest
            .operator_states
            .insert("big-op".into(), OperatorCheckpoint::external(0, 4096));

        store
            .save_with_state(
                &manifest,
                Some(&[bytes::Bytes::copy_from_slice(&large_data)]),
            )
            .await
            .unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(None).await.unwrap().unwrap();

        let small = result.manifest.operator_states.get("small-op").unwrap();
        assert_eq!(small.decode_inline().unwrap(), b"tiny");

        let big = result.manifest.operator_states.get("big-op").unwrap();
        assert_eq!(big.decode_inline().unwrap(), large_data);
    }

    #[tokio::test]
    async fn test_recover_missing_sidecar_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Manifest references external state but sidecar is missing
        let mut manifest = finalized_manifest(1, 1);
        manifest
            .operator_states
            .insert("orphan".into(), OperatorCheckpoint::external(0, 100));
        store
            .save_with_state(&manifest, Some(&[bytes::Bytes::from(vec![0; 100])]))
            .await
            .unwrap();
        std::fs::remove_file(dir.path().join("checkpoints/checkpoint_000001/state.bin")).unwrap();

        let mgr = RecoveryManager::new(&store);
        let error = mgr.recover(None).await.unwrap_err();
        assert!(error.to_string().contains("checkpoint history exists"));
    }

    #[tokio::test]
    async fn prepared_manifest_without_decision_is_not_recoverable() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        store.save(&CheckpointManifest::new(1, 1)).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let error = mgr.recover(None).await.unwrap_err();
        assert!(error.to_string().contains("checkpoint history exists"));
    }

    #[tokio::test]
    async fn prepared_manifest_without_outcomes_recovers_genesis() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        store.save(&CheckpointManifest::new(1, 1)).await.unwrap();
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );

        let recovered = RecoveryManager::new(&store)
            .recover(Some(&decisions))
            .await
            .unwrap();

        assert!(recovered.is_none());
        assert_eq!(
            store.load_by_id(1).await.unwrap().unwrap().durable_phase,
            DurableCheckpointPhase::Prepared
        );
        let settled = decisions.outcome(1).await.unwrap().unwrap();
        assert_eq!(settled.checkpoint_id, 1);
        assert_eq!(settled.verdict, CheckpointVerdict::Abort);
    }

    #[tokio::test]
    async fn recovery_settles_newer_prepared_attempt_before_restoring_older_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut committed = CheckpointManifest::new(1, 5);
        committed.deployment_id.clone_from(&deployment_id);
        store.save(&committed).await.unwrap();
        let mut unresolved = CheckpointManifest::new(2, 7);
        unresolved.deployment_id.clone_from(&deployment_id);
        store.save(&unresolved).await.unwrap();
        record_local_commit(&decisions, 5, 1).await;

        let recovered = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .recover(Some(&decisions))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            (recovered.epoch(), recovered.manifest.checkpoint_id),
            (5, 1)
        );
        let settled = decisions.outcome(7).await.unwrap().unwrap();
        assert_eq!(settled.checkpoint_id, 2);
        assert_eq!(settled.verdict, CheckpointVerdict::Abort);
    }

    #[tokio::test]
    async fn recovery_settles_outcome_less_prepared_below_a_later_closed_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut committed = CheckpointManifest::new(1, 5);
        committed.deployment_id.clone_from(&deployment_id);
        store.save(&committed).await.unwrap();
        let mut unresolved = CheckpointManifest::new(2, 6);
        unresolved.deployment_id.clone_from(&deployment_id);
        store.save(&unresolved).await.unwrap();
        let mut later_aborted = CheckpointManifest::new(3, 7);
        later_aborted.deployment_id.clone_from(&deployment_id);
        store.save(&later_aborted).await.unwrap();
        record_local_commit(&decisions, 5, 1).await;
        record_local_abort(&decisions, 7, 3).await;

        let recovered = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .recover(Some(&decisions))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(recovered.manifest.checkpoint_id, 1);
        let settled = decisions.outcome(6).await.unwrap().unwrap();
        assert_eq!(settled.checkpoint_id, 2);
        assert_eq!(settled.verdict, CheckpointVerdict::Abort);
    }

    #[tokio::test]
    async fn exact_commit_winner_is_accepted_when_recovery_abort_loses() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut prepared = CheckpointManifest::new(1, 7);
        prepared.deployment_id.clone_from(&deployment_id);
        store.save(&prepared).await.unwrap();
        record_local_commit(&decisions, 7, 1).await;

        let manager = RecoveryManager::new(&store).with_deployment_id(&deployment_id);
        manager
            .settle_local_prepared_attempts(&decisions, &[])
            .await
            .expect("the exact Commit winner must defeat the stale Abort attempt");
        let recovered = manager.recover(Some(&decisions)).await.unwrap().unwrap();

        assert_eq!(recovered.manifest.checkpoint_id, 1);
        assert_eq!(
            recovered.manifest.durable_phase,
            DurableCheckpointPhase::Finalized
        );
        assert!(decisions.outcome(7).await.unwrap().unwrap().is_commit());
    }

    #[tokio::test]
    async fn recovery_does_not_mint_abort_from_invalid_prepared_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut invalid = CheckpointManifest::new(1, 7);
        invalid.deployment_id.clone_from(&deployment_id);
        invalid.timestamp_ms = 0;
        store.save(&invalid).await.unwrap();

        let error = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .recover(Some(&decisions))
            .await
            .expect_err("invalid Prepared inventory cannot authorize an Abort");

        assert!(error
            .to_string()
            .contains("cannot be used to mint an Abort"));
        assert!(decisions.outcome(7).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn recovery_does_not_mint_abort_from_foreign_prepared_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let foreign_authority = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let mut foreign = CheckpointManifest::new(1, 7);
        foreign.deployment_id = foreign_authority
            .load_or_create_deployment_id()
            .await
            .unwrap();
        store.save(&foreign).await.unwrap();

        let error = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .recover(Some(&decisions))
            .await
            .expect_err("foreign Prepared inventory cannot authorize an Abort");

        assert!(error
            .to_string()
            .contains("active deployment/pipeline identity"));
        assert!(decisions.outcome(7).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn recovery_rejects_newer_finalized_attempt_without_exact_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut committed = CheckpointManifest::new(1, 5);
        committed.deployment_id.clone_from(&deployment_id);
        store.save(&committed).await.unwrap();
        let mut newer = finalized_manifest(2, 7);
        newer.deployment_id.clone_from(&deployment_id);
        store.save(&newer).await.unwrap();
        record_local_commit(&decisions, 5, 1).await;

        let error = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .recover(Some(&decisions))
            .await
            .expect_err("newer Finalized state cannot be bypassed by an older Commit");

        assert!(
            error
                .to_string()
                .contains("newer Finalized checkpoint 2 epoch 7 is not settled"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn commit_followed_by_abort_restores_the_commit_cut() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let outcomes = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = outcomes.load_or_create_deployment_id().await.unwrap();

        let mut committed = CheckpointManifest::new(1, 5);
        committed.deployment_id.clone_from(&deployment_id);
        store.save(&committed).await.unwrap();
        let mut aborted = CheckpointManifest::new(2, 7);
        aborted.deployment_id.clone_from(&deployment_id);
        store.save(&aborted).await.unwrap();
        record_local_commit(&outcomes, 5, 1).await;
        record_local_abort(&outcomes, 7, 2).await;

        let recovered = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .recover(Some(&outcomes))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(recovered.epoch(), 5);
        assert!(matches!(
            recovered.outcome.as_ref().unwrap().verdict,
            CheckpointVerdict::Commit
        ));
    }

    #[tokio::test]
    async fn abort_only_history_has_no_recovery_cut() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let outcomes = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = outcomes.load_or_create_deployment_id().await.unwrap();
        let mut aborted = CheckpointManifest::new(1, 5);
        aborted.deployment_id.clone_from(&deployment_id);
        store.save(&aborted).await.unwrap();
        record_local_abort(&outcomes, 5, 1).await;

        let recovered = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .recover(Some(&outcomes))
            .await
            .unwrap();

        assert!(recovered.is_none());
        assert_eq!(
            store.load_by_id(1).await.unwrap().unwrap().durable_phase,
            DurableCheckpointPhase::Prepared
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_recovery_accepts_historical_leader_proof_structurally() {
        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let store =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let fence = assignment_fence(4, &[1]);
        let outcomes = cluster_decisions(backing, &fence, 1).await;
        let deployment_id = outcomes.load_or_create_deployment_id().await.unwrap();
        let mut manifest = CheckpointManifest::new(7, 6);
        manifest.participant_id = 1;
        manifest.deployment_id.clone_from(&deployment_id);
        store.save(&manifest).await.unwrap();
        record_cluster_commit(&outcomes, 6, 7, &fence, 1, Some(&manifest)).await;

        let recovered = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover_cluster(&outcomes.authority, &outcomes.capsules)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(recovered.epoch(), 6);
        assert_eq!(
            recovered
                .outcome()
                .unwrap()
                .leader_proof
                .as_ref()
                .unwrap()
                .fencing_token,
            1
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn outcome_floor_does_not_dominate_a_newer_prepared_checkpoint_id() {
        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let offline =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let donor =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
                .with_participant_id(2);
        let fence = assignment_fence(4, &[2]);
        let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 2).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

        record_cluster_commit(&decisions, 5, 1, &fence, 2, None).await;
        let mut retained = CheckpointManifest::new(3, 7);
        retained.participant_id = 2;
        retained.deployment_id.clone_from(&deployment_id);
        donor.save(&retained).await.unwrap();
        record_cluster_commit(&decisions, 7, 3, &fence, 2, Some(&retained)).await;
        decisions
            .authority
            .prune_cluster_outcomes_before(&decisions.proof, 7, |_| async { Ok(()) })
            .await
            .unwrap();
        assert_eq!(
            decisions
                .authority
                .cluster_outcome_retention_boundary()
                .await
                .unwrap()
                .artifact_before_epoch,
            7
        );
        assert!(decisions
            .authority
            .cluster_outcome(5)
            .await
            .unwrap()
            .is_none());

        let mut stale = CheckpointManifest::new(99, 6);
        stale.participant_id = 1;
        stale.deployment_id.clone_from(&deployment_id);
        offline.save(&stale).await.unwrap();

        let error = RecoveryManager::new(&offline)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover_cluster(&decisions.authority, &decisions.capsules)
            .await
            .expect_err("an outcome floor cannot settle an incomparable Prepared attempt");
        assert!(
            error
                .to_string()
                .contains("checkpoint 99 epoch 6 is not settled by an exact terminal outcome"),
            "{error}"
        );
        assert_eq!(
            offline.load_by_id(99).await.unwrap().unwrap().durable_phase,
            DurableCheckpointPhase::Prepared
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn later_terminal_strictly_dominates_missing_prepared_attempt() {
        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let store =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let fence = assignment_fence(4, &[1]);
        let decisions = cluster_decisions(backing, &fence, 1).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

        let mut committed = CheckpointManifest::new(5, 5);
        committed.participant_id = 1;
        committed.deployment_id.clone_from(&deployment_id);
        store.save(&committed).await.unwrap();
        record_cluster_commit(&decisions, 5, 5, &fence, 1, Some(&committed)).await;
        let mut prepared = CheckpointManifest::new(6, 6);
        prepared.participant_id = 1;
        prepared.deployment_id.clone_from(&deployment_id);
        store.save(&prepared).await.unwrap();
        record_cluster_abort(&decisions, 7, 7, &fence).await;

        let recovered = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover_cluster(&decisions.authority, &decisions.capsules)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            (recovered.epoch(), recovered.manifest.checkpoint_id),
            (5, 5)
        );
        assert_eq!(
            store.load_by_id(6).await.unwrap().unwrap().durable_phase,
            DurableCheckpointPhase::Prepared
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn incomparable_missing_prepared_attempt_remains_fatal() {
        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let store =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let fence = assignment_fence(4, &[1]);
        let decisions = cluster_decisions(backing, &fence, 1).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

        let mut committed = CheckpointManifest::new(5, 5);
        committed.participant_id = 1;
        committed.deployment_id.clone_from(&deployment_id);
        store.save(&committed).await.unwrap();
        record_cluster_commit(&decisions, 5, 5, &fence, 1, Some(&committed)).await;
        let mut incomparable = CheckpointManifest::new(8, 6);
        incomparable.participant_id = 1;
        incomparable.deployment_id.clone_from(&deployment_id);
        store.save(&incomparable).await.unwrap();
        record_cluster_abort(&decisions, 7, 7, &fence).await;

        let error = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover_cluster(&decisions.authority, &decisions.capsules)
            .await
            .expect_err("incomparable dimensions cannot be inferred closed");

        assert!(
            error
                .to_string()
                .contains("checkpoint 8 epoch 6 is not settled by an exact terminal outcome"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn dominated_prepared_attempt_with_foreign_provenance_remains_fatal() {
        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let store =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let fence = assignment_fence(4, &[1]);
        let decisions = cluster_decisions(backing, &fence, 1).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

        let mut committed = CheckpointManifest::new(5, 5);
        committed.participant_id = 1;
        committed.deployment_id.clone_from(&deployment_id);
        store.save(&committed).await.unwrap();
        record_cluster_commit(&decisions, 5, 5, &fence, 1, Some(&committed)).await;
        let mut foreign = CheckpointManifest::new(6, 6);
        foreign.participant_id = 1;
        foreign.deployment_id = uuid::Uuid::new_v4().to_string();
        store.save(&foreign).await.unwrap();
        record_cluster_abort(&decisions, 7, 7, &fence).await;

        let error = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover_cluster(&decisions.authority, &decisions.capsules)
            .await
            .expect_err("dominance cannot hide foreign recovery inventory");

        assert!(
            error
                .to_string()
                .contains("does not belong to storage participant 1 and the active deployment/pipeline identity"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_recovery_ignores_forged_standalone_outcome_keys() {
        use object_store::{ObjectStoreExt, PutPayload};

        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let store =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let fence = assignment_fence(4, &[1]);
        let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut manifest = CheckpointManifest::new(7, 6);
        manifest.participant_id = 1;
        manifest.deployment_id.clone_from(&deployment_id);
        store.save(&manifest).await.unwrap();
        record_cluster_commit(&decisions, 6, 7, &fence, 1, Some(&manifest)).await;

        let mut forged = decisions
            .authority
            .cluster_outcome(6)
            .await
            .unwrap()
            .unwrap();
        forged.epoch = 9;
        forged.checkpoint_id = 99;
        backing
            .put(
                &object_store::path::Path::from("checkpoint-outcomes/epoch=9/outcome"),
                PutPayload::from_bytes(bytes::Bytes::from(serde_json::to_vec(&forged).unwrap())),
            )
            .await
            .unwrap();

        let recovered = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover_cluster(&decisions.authority, &decisions.capsules)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (recovered.epoch(), recovered.manifest.checkpoint_id),
            (6, 7)
        );
    }

    #[tokio::test]
    async fn published_finalized_manifest_fails_without_commit_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        store.save(&finalized_manifest(1, 1)).await.unwrap();
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );

        let error = RecoveryManager::new(&store)
            .recover(Some(&decisions))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains(
                "published finalized checkpoint 1 epoch 1 exists but no durable Commit outcome exists"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn finalized_manifest_without_latest_pointer_fails_without_commit_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        store.save(&finalized_manifest(1, 1)).await.unwrap();
        std::fs::remove_file(dir.path().join("checkpoints/latest.txt")).unwrap();
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );

        let error = RecoveryManager::new(&store)
            .recover(Some(&decisions))
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("finalized checkpoint 1 epoch 1 exists in recovery inventory"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn dangling_latest_pointer_fails_without_commit_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        store.save(&CheckpointManifest::new(1, 1)).await.unwrap();
        std::fs::write(
            dir.path().join("checkpoints/latest.txt"),
            "checkpoint_000099",
        )
        .unwrap();
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );

        let error = RecoveryManager::new(&store)
            .recover(Some(&decisions))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains(
                "checkpoint recovery pointer is invalid while no durable Commit outcome exists"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn unreadable_manifest_fails_without_commit_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        store.save(&CheckpointManifest::new(1, 1)).await.unwrap();
        std::fs::write(
            dir.path()
                .join("checkpoints/checkpoint_000001/manifest.json"),
            b"not-json",
        )
        .unwrap();
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );

        let error = RecoveryManager::new(&store)
            .recover(Some(&decisions))
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("checkpoint 1 recovery inventory is unreadable"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn committed_prepared_manifest_is_finalized_and_recovered() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut prepared = CheckpointManifest::new(1, 7);
        prepared.deployment_id.clone_from(&deployment_id);
        store.save(&prepared).await.unwrap();
        record_local_commit(&decisions, 7, 1).await;

        let recovered = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .recover(Some(&decisions))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            recovered.manifest.durable_phase,
            DurableCheckpointPhase::Finalized
        );
        assert_eq!(recovered.epoch(), 7);
        assert_eq!(
            store.load_by_id(1).await.unwrap().unwrap().durable_phase,
            DurableCheckpointPhase::Finalized
        );
    }

    #[tokio::test]
    async fn finalized_manifest_requires_exact_commit_outcome_when_store_is_configured() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut manifest = finalized_manifest(1, 1);
        manifest.deployment_id.clone_from(&deployment_id);
        store.save(&manifest).await.unwrap();

        let error = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .recover(Some(&decisions))
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("published finalized checkpoint 1 epoch 1 exists"));
    }

    #[tokio::test]
    async fn exact_outcome_must_match_manifest_deployment() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let decision_deployment = decisions.load_or_create_deployment_id().await.unwrap();
        let other_namespace = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let manifest_deployment = other_namespace
            .load_or_create_deployment_id()
            .await
            .unwrap();
        assert_ne!(decision_deployment, manifest_deployment);

        let mut manifest = finalized_manifest(1, 1);
        manifest.deployment_id.clone_from(&manifest_deployment);
        store.save(&manifest).await.unwrap();
        record_local_commit(&decisions, 1, 1).await;

        let error = RecoveryManager::new(&store)
            .with_deployment_id(&manifest_deployment)
            .recover(Some(&decisions))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("local decision authority deployment"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn pipeline_identity_mismatch_is_fatal_without_older_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let expected = pipeline_identity(0x11);

        let mut older = finalized_manifest(1, 1);
        older.pipeline_identity = expected.clone();
        store.save(&older).await.unwrap();

        let mut latest = finalized_manifest(2, 2);
        latest.pipeline_identity = pipeline_identity(0x22);
        store.save(&latest).await.unwrap();

        let error = RecoveryManager::new(&store)
            .with_pipeline_identity(&expected)
            .recover(None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("[LDB-6043]"));
        assert!(error.to_string().contains("checkpoint 2"));
    }

    #[tokio::test]
    async fn identity_mismatch_does_not_finalize_committed_prepared_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut prepared = CheckpointManifest::new(1, 7);
        prepared.deployment_id.clone_from(&deployment_id);
        prepared.pipeline_identity = pipeline_identity(0x33);
        store.save(&prepared).await.unwrap();

        record_local_commit(&decisions, 7, 1).await;

        let error = RecoveryManager::new(&store)
            .with_deployment_id(&deployment_id)
            .with_pipeline_identity(&pipeline_identity(0x44))
            .recover(Some(&decisions))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("[LDB-6043]"));
        let persisted = store.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(persisted.durable_phase, DurableCheckpointPhase::Prepared);
    }

    #[tokio::test]
    async fn outcome_scope_mismatch_does_not_finalize_prepared_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let prepared = CheckpointManifest::new(1, 7);
        store.save(&prepared).await.unwrap();

        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        record_local_commit(&decisions, 7, 1).await;

        let error = RecoveryManager::new(&store)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover(Some(&decisions))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("recovery authority scope Local"));
        let persisted = store.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(persisted.durable_phase, DurableCheckpointPhase::Prepared);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn local_recovery_rejects_cluster_outcome_before_manifest_access() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let fence = assignment_fence(4, &[1]);
        let decisions = cluster_decisions(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            &fence,
            1,
        )
        .await;
        record_cluster_commit(&decisions, 7, 1, &fence, 1, None).await;

        let error = RecoveryManager::new(&store)
            .recover_cluster(&decisions.authority, &decisions.capsules)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("recovery authority scope Cluster"));
        assert!(error.to_string().contains("active runtime scope Local"));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn excluded_participant_recovers_exact_portable_peer_manifest() {
        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let donor =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let local =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
                .with_participant_id(2);
        let fence = assignment_fence(4, &[1, 3]);
        let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

        let mut manifest = CheckpointManifest::new(7, 6);
        manifest.participant_id = 1;
        manifest.deployment_id.clone_from(&deployment_id);
        manifest.source_offsets.insert(
            "events".into(),
            ConnectorCheckpoint {
                offsets: HashMap::from([("partition:0".into(), "41".into())]),
                metadata: HashMap::from([("topic".into(), "events".into())]),
                source_assignment_version: std::num::NonZeroU64::new(4),
            },
        );
        manifest.source_names.push("events".into());
        manifest.source_watermarks.insert("events".into(), 42_000);
        manifest.watermark = Some(40_000);
        manifest
            .operator_states
            .insert("global".into(), OperatorCheckpoint::external(0, 5));
        let manifest = donor
            .save_with_state(&manifest, Some(&[Bytes::from_static(b"state")]))
            .await
            .unwrap();
        record_cluster_commit(&decisions, 6, 7, &fence, 1, Some(&manifest)).await;

        let manager = RecoveryManager::new(&local)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster);
        let recovered = manager
            .recover_cluster_to_epoch(6, &decisions.authority, &decisions.capsules)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(recovered.manifest.participant_id, 1);
        assert_eq!(
            recovered.manifest.source_offsets["events"].offsets["partition:0"],
            "41"
        );
        assert_eq!(
            recovered.manifest.source_offsets["events"].metadata["topic"],
            "events"
        );
        assert_eq!(recovered.manifest.source_watermarks["events"], 42_000);
        assert_eq!(recovered.manifest.watermark, Some(40_000));
        assert_eq!(
            recovered.manifest.operator_states["global"]
                .decode_inline()
                .unwrap(),
            b"state"
        );
        assert_eq!(
            recovered
                .cluster_capsule()
                .unwrap()
                .participants
                .iter()
                .map(|participant| participant.participant_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(
            donor.load_by_id(7).await.unwrap().unwrap().durable_phase,
            DurableCheckpointPhase::Prepared,
            "peer recovery must not rewrite another participant's recovery pointer"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_recovery_prefers_the_local_replica_over_a_valid_lower_id_peer() {
        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let peer =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let local =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
                .with_participant_id(2);
        let fence = assignment_fence(4, &[1, 2]);
        let decisions = cluster_decisions(backing, &fence, 1).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

        let mut participant_1 = CheckpointManifest::new(7, 6);
        participant_1.participant_id = 1;
        participant_1.deployment_id.clone_from(&deployment_id);
        let mut participant_2 = participant_1.clone();
        participant_2.participant_id = 2;
        peer.save(&participant_1).await.unwrap();
        local.save(&participant_2).await.unwrap();

        record_cluster_commit_for_manifests(
            &decisions,
            6,
            7,
            &fence,
            &[(1, &participant_1), (2, &participant_2)],
        )
        .await;

        let recovered = RecoveryManager::new(&local)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover_cluster(&decisions.authority, &decisions.capsules)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(recovered.manifest.participant_id, 2);
        assert_eq!(
            local.load_by_id(7).await.unwrap().unwrap().durable_phase,
            DurableCheckpointPhase::Finalized
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_recovery_uses_a_peer_when_the_local_sidecar_is_corrupt() {
        use object_store::{PutOptions, PutPayload};

        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let peer =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let local =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
                .with_participant_id(2);
        let fence = assignment_fence(4, &[1, 2]);
        let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

        let mut participant_1 = CheckpointManifest::new(7, 6);
        participant_1.participant_id = 1;
        participant_1.deployment_id.clone_from(&deployment_id);
        participant_1
            .operator_states
            .insert("global".into(), OperatorCheckpoint::external(0, 5));
        let mut participant_2 = participant_1.clone();
        participant_2.participant_id = 2;
        let participant_1 = peer
            .save_with_state(&participant_1, Some(&[Bytes::from_static(b"state")]))
            .await
            .unwrap();
        let participant_2 = local
            .save_with_state(&participant_2, Some(&[Bytes::from_static(b"state")]))
            .await
            .unwrap();

        record_cluster_commit_for_manifests(
            &decisions,
            6,
            7,
            &fence,
            &[(1, &participant_1), (2, &participant_2)],
        )
        .await;
        backing
            .put_opts(
                &object_store::path::Path::from("nodes/2/checkpoints/state-000007.bin"),
                PutPayload::from_bytes(Bytes::from_static(b"other")),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let recovered = RecoveryManager::new(&local)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover_cluster(&decisions.authority, &decisions.capsules)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(recovered.manifest.participant_id, 1);
        assert_eq!(
            recovered.manifest.operator_states["global"]
                .decode_inline()
                .unwrap(),
            b"state"
        );
        assert_eq!(
            peer.load_by_id(7).await.unwrap().unwrap().durable_phase,
            DurableCheckpointPhase::Prepared,
            "peer recovery must not publish the peer's prepared manifest"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn retention_preflight_uses_a_complete_peer_and_rejects_all_missing_sidecars() {
        use object_store::ObjectStoreExt;

        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let peer =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let local =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
                .with_participant_id(2);
        let fence = assignment_fence(4, &[1, 2]);
        let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

        let mut participant_1 = CheckpointManifest::new(7, 6);
        participant_1.participant_id = 1;
        participant_1.deployment_id.clone_from(&deployment_id);
        participant_1
            .operator_states
            .insert("global".into(), OperatorCheckpoint::external(0, 5));
        let mut participant_2 = participant_1.clone();
        participant_2.participant_id = 2;
        let participant_1 = peer
            .save_with_state(&participant_1, Some(&[Bytes::from_static(b"state")]))
            .await
            .unwrap();
        let participant_2 = local
            .save_with_state(&participant_2, Some(&[Bytes::from_static(b"state")]))
            .await
            .unwrap();
        record_cluster_commit_for_manifests(
            &decisions,
            6,
            7,
            &fence,
            &[(1, &participant_1), (2, &participant_2)],
        )
        .await;
        let outcome = decisions
            .authority
            .cluster_outcome(6)
            .await
            .unwrap()
            .expect("cluster Commit outcome");
        let capsule = decisions
            .load_recovery_capsule(outcome.recovery_capsule.as_ref().unwrap())
            .await
            .unwrap();
        let manager = RecoveryManager::new(&local)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster);

        backing
            .delete(&object_store::path::Path::from(
                "nodes/2/checkpoints/state-000007.bin",
            ))
            .await
            .unwrap();
        manager
            .preflight_cluster_committed_metadata(&outcome, &capsule)
            .await
            .expect("a complete peer sidecar must preserve the recovery cut");

        backing
            .delete(&object_store::path::Path::from(
                "nodes/1/checkpoints/state-000007.bin",
            ))
            .await
            .unwrap();
        let error = manager
            .preflight_cluster_committed_metadata(&outcome, &capsule)
            .await
            .expect_err("retention must fail when every participant sidecar is missing");
        assert!(
            error
                .to_string()
                .contains("no usable participant manifest metadata; 2 candidate(s) rejected"),
            "{error}"
        );
        assert!(error.to_string().contains("sidecar is absent"), "{error}");
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_recovery_never_combines_a_manifest_and_sidecar_from_different_participants() {
        use object_store::ObjectStoreExt;

        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let peer =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let local =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
                .with_participant_id(2);
        let fence = assignment_fence(4, &[1, 2]);
        let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

        let mut participant_1 = CheckpointManifest::new(7, 6);
        participant_1.participant_id = 1;
        participant_1.deployment_id.clone_from(&deployment_id);
        participant_1
            .operator_states
            .insert("global".into(), OperatorCheckpoint::external(0, 5));
        let mut participant_2 = participant_1.clone();
        participant_2.participant_id = 2;
        let participant_1 = peer
            .save_with_state(&participant_1, Some(&[Bytes::from_static(b"state")]))
            .await
            .unwrap();
        let participant_2 = local
            .save_with_state(&participant_2, Some(&[Bytes::from_static(b"state")]))
            .await
            .unwrap();

        record_cluster_commit_for_manifests(
            &decisions,
            6,
            7,
            &fence,
            &[(1, &participant_1), (2, &participant_2)],
        )
        .await;
        backing
            .delete(&object_store::path::Path::from(
                "nodes/1/checkpoints/state-000007.bin",
            ))
            .await
            .unwrap();
        backing
            .delete(&object_store::path::Path::from(
                "nodes/2/manifests/manifest-000007.json",
            ))
            .await
            .unwrap();

        let error = RecoveryManager::new(&local)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover_cluster(&decisions.authority, &decisions.capsules)
            .await
            .expect_err("recovery must keep each participant's artifacts paired");

        assert!(
            error
                .to_string()
                .contains("no usable participant artifact replica; 2 candidate(s) rejected"),
            "{error}"
        );
        assert!(
            error
                .to_string()
                .contains("first failure: participant 2 manifest is absent"),
            "{error}"
        );
        assert!(
            error
                .to_string()
                .contains("last failure: participant 1 artifact integrity failed"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_recovery_rejects_changed_only_artifact_replica() {
        use object_store::{PutOptions, PutPayload};

        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let donor =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let local =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
                .with_participant_id(2);
        let fence = assignment_fence(4, &[1, 3]);
        let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut manifest = CheckpointManifest::new(7, 6);
        manifest.participant_id = 1;
        manifest.deployment_id.clone_from(&deployment_id);
        donor.save(&manifest).await.unwrap();
        record_cluster_commit(&decisions, 6, 7, &fence, 1, Some(&manifest)).await;
        manifest.source_watermarks.insert("events".into(), 42_000);
        backing
            .put_opts(
                &object_store::path::Path::from("nodes/1/manifests/manifest-000007.json"),
                PutPayload::from_bytes(bytes::Bytes::from(
                    serde_json::to_vec_pretty(&manifest).unwrap(),
                )),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let error = RecoveryManager::new(&local)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover_cluster(&decisions.authority, &decisions.capsules)
            .await
            .expect_err("the capsule must reject a changed participant manifest");

        assert!(
            error
                .to_string()
                .contains("participant 1 manifest digest does not match the recovery capsule"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn committed_peer_manifest_must_match_the_exact_epoch() {
        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let donor =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let local =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
                .with_participant_id(2);
        let fence = assignment_fence(4, &[1, 3]);
        let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut wrong_epoch = CheckpointManifest::new(7, 5);
        wrong_epoch.participant_id = 1;
        wrong_epoch.deployment_id.clone_from(&deployment_id);
        donor.save(&wrong_epoch).await.unwrap();
        record_cluster_commit(&decisions, 6, 7, &fence, 1, Some(&wrong_epoch)).await;

        let error = RecoveryManager::new(&local)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover_cluster_to_epoch(6, &decisions.authority, &decisions.capsules)
            .await
            .expect_err("outcome and donor manifest must identify one exact attempt");

        assert!(
            error
                .to_string()
                .contains("participant 1 manifest does not identify the committed runtime cut"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn all_capsule_participant_artifacts_missing_fails_closed() {
        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let local =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
                .with_participant_id(2);
        let fence = assignment_fence(4, &[1, 2]);
        let decisions = cluster_decisions(backing, &fence, 1).await;
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        record_cluster_commit(&decisions, 6, 7, &fence, 1, None).await;

        let error = RecoveryManager::new(&local)
            .with_deployment_id(&deployment_id)
            .with_outcome_scope(CheckpointScope::Cluster)
            .recover_cluster(&decisions.authority, &decisions.capsules)
            .await
            .expect_err("a Commit cannot fall back to genesis when every replica is absent");

        assert!(
            error
                .to_string()
                .contains("no usable participant artifact replica; 2 candidate(s) rejected"),
            "{error}"
        );
    }
}

#[cfg(test)]
mod rehydration_tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use laminar_core::state::{
        InProcessBackend, ObjectStoreBackend, StateBackendDurability, StateBackendError,
    };

    struct CorruptReadBackend {
        inner: InProcessBackend,
        corrupt_attempt: CheckpointAttempt,
    }

    #[async_trait]
    impl StateBackend for CorruptReadBackend {
        fn key_group_capacity(&self) -> u32 {
            self.inner.key_group_capacity()
        }

        async fn write_partial(
            &self,
            attempt: CheckpointAttempt,
            vnode: u32,
            assignment_version: u64,
            bytes: Bytes,
        ) -> Result<(), StateBackendError> {
            self.inner
                .write_partial(attempt, vnode, assignment_version, bytes)
                .await
        }

        async fn read_partial(
            &self,
            attempt: CheckpointAttempt,
            vnode: u32,
        ) -> Result<Option<Bytes>, StateBackendError> {
            let Some(bytes) = self.inner.read_partial(attempt, vnode).await? else {
                return Ok(None);
            };
            if attempt != self.corrupt_attempt || bytes.is_empty() {
                return Ok(Some(bytes));
            }
            let mut corrupt = bytes.to_vec();
            corrupt[0] ^= 0xff;
            Ok(Some(Bytes::from(corrupt)))
        }

        async fn write_commit_descriptor(
            &self,
            attempt: CheckpointAttempt,
            key: &str,
            bytes: Bytes,
        ) -> Result<(), StateBackendError> {
            self.inner
                .write_commit_descriptor(attempt, key, bytes)
                .await
        }

        async fn read_commit_descriptor(
            &self,
            attempt: CheckpointAttempt,
            key: &str,
        ) -> Result<Option<Bytes>, StateBackendError> {
            self.inner.read_commit_descriptor(attempt, key).await
        }

        async fn read_sealed_commit_descriptor_bounded(
            &self,
            attempt: CheckpointAttempt,
            sealed: &laminar_core::state::SealedCommitDescriptor,
            max_bytes: u64,
        ) -> Result<Option<Bytes>, StateBackendError> {
            self.inner
                .read_sealed_commit_descriptor_bounded(attempt, sealed, max_bytes)
                .await
        }

        async fn seal_checkpoint(
            &self,
            attempt: CheckpointAttempt,
            assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
            vnodes: &[u32],
            required_descriptors: &[String],
        ) -> Result<bool, StateBackendError> {
            self.inner
                .seal_checkpoint(attempt, assignment_fence, vnodes, required_descriptors)
                .await
        }

        async fn checkpoint_seal_inventory(
            &self,
            attempt: CheckpointAttempt,
        ) -> Result<Option<CheckpointSealInventory>, StateBackendError> {
            self.inner.checkpoint_seal_inventory(attempt).await
        }

        async fn verify_checkpoint_artifact_metadata(
            &self,
            inventory: &CheckpointSealInventory,
        ) -> Result<(), StateBackendError> {
            self.inner
                .verify_checkpoint_artifact_metadata(inventory)
                .await
        }

        async fn prune_before(&self, before: u64) -> Result<(), StateBackendError> {
            self.inner.prune_before(before).await
        }

        fn durability_scope(&self) -> StateBackendDurability {
            self.inner.durability_scope()
        }
    }

    async fn seal_epoch(backend: &dyn StateBackend, epoch: u64, vnodes: &[u32], tag: &[u8]) {
        let attempt = CheckpointAttempt::new(epoch, epoch);
        for &v in vnodes {
            let partial = crate::vnode_partial::VnodePartial {
                operators: vec![("agg".into(), tag.to_vec())],
                base: None,
                deltas: Vec::new(),
            };
            backend
                .write_partial(attempt, v, 0, Bytes::from(partial.encode().unwrap()))
                .await
                .unwrap();
        }
        assert!(backend
            .seal_checkpoint(attempt, None, vnodes, &[])
            .await
            .unwrap());
    }

    #[cfg(feature = "cluster")]
    async fn write_and_seal_partial(
        backend: &dyn StateBackend,
        attempt: CheckpointAttempt,
        partial: crate::vnode_partial::VnodePartial,
    ) {
        backend
            .write_partial(attempt, 0, 0, Bytes::from(partial.encode().unwrap()))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(attempt, None, &[0], &[])
            .await
            .unwrap());
    }

    fn operator_payload(report: &VnodeRehydration, vnode: u32) -> Vec<u8> {
        let bytes = &report.restored.get(&vnode).unwrap()[0];
        let partial = crate::vnode_partial::VnodePartial::decode(bytes).unwrap();
        partial.operators[0].1.clone()
    }

    #[tokio::test]
    async fn rehydrate_reads_committed_partials_and_rejects_missing_vnodes() {
        let backend = InProcessBackend::new(4);
        seal_epoch(&backend, 7, &[0, 1, 2], b"v7").await;

        let report = VnodeRehydrator::new(&backend)
            .rehydrate_at(&[0, 1], CheckpointAttempt::new(7, 7))
            .await
            .unwrap();

        assert_eq!(report.attempt, Some(CheckpointAttempt::new(7, 7)));
        assert_eq!(report.restored_count(), 2);
        assert_eq!(operator_payload(&report, 0), b"v7");
        assert_eq!(operator_payload(&report, 1), b"v7");

        let error = VnodeRehydrator::new(&backend)
            .rehydrate_at(&[3], CheckpointAttempt::new(7, 7))
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("absent from the exact state seal"));
    }

    #[tokio::test]
    async fn rehydrate_rejects_payload_that_no_longer_matches_seal_digest() {
        let attempt = CheckpointAttempt::new(7, 7);
        let backend = CorruptReadBackend {
            inner: InProcessBackend::new(4),
            corrupt_attempt: attempt,
        };
        seal_epoch(&backend, 7, &[0], b"original").await;

        let error = VnodeRehydrator::new(&backend)
            .rehydrate_at(&[0], attempt)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("payload does not match the exact state seal"));
    }

    /// Boot recovery pins the read to the recovered manifest's epoch so state and source offsets
    /// resume from one cut, even when a later epoch sealed.
    #[tokio::test]
    async fn rehydrate_at_pins_the_requested_epoch() {
        let backend = InProcessBackend::new(4);
        seal_epoch(&backend, 3, &[0, 1], b"old").await;
        seal_epoch(&backend, 9, &[0, 1], b"new").await;

        let report = VnodeRehydrator::new(&backend)
            .rehydrate_at(&[0, 1], CheckpointAttempt::new(3, 3))
            .await
            .unwrap();

        assert_eq!(report.attempt, Some(CheckpointAttempt::new(3, 3)));
        assert_eq!(operator_payload(&report, 0), b"old");
    }

    /// A reference partial resolves (one hop) to the
    /// full partial it points at.
    #[tokio::test]
    async fn rehydrate_resolves_reference_partials() {
        let backend = InProcessBackend::new(4);

        let full = crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), vec![1, 2, 3])],
            base: None,
            deltas: Vec::new(),
        };
        let full_attempt = CheckpointAttempt::new(5, 1);
        backend
            .write_partial(full_attempt, 0, 0, Bytes::from(full.encode().unwrap()))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(full_attempt, None, &[0], &[])
            .await
            .unwrap());

        let reference = crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(full_attempt),
            deltas: Vec::new(),
        };
        let reference_attempt = CheckpointAttempt::new(6, 2);
        backend
            .write_partial(
                reference_attempt,
                0,
                0,
                Bytes::from(reference.encode().unwrap()),
            )
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(reference_attempt, None, &[0], &[])
            .await
            .unwrap());

        let report = VnodeRehydrator::new(&backend)
            .rehydrate_at(&[0], reference_attempt)
            .await
            .unwrap();
        assert_eq!(report.attempt, Some(reference_attempt));
        let chain = report.restored.get(&0).expect("vnode restored");
        assert_eq!(chain.len(), 1, "reference resolves to a single full base");
        let restored = crate::vnode_partial::VnodePartial::decode(&chain[0]).unwrap();
        assert_eq!(
            restored.base, None,
            "the resolved partial must be the full base, not the reference",
        );
        assert_eq!(restored.operators[0].1, vec![1, 2, 3]);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn rehydrate_resolves_sealed_full_delta_delta_chain() {
        let backend = InProcessBackend::new(1);
        let full_attempt = CheckpointAttempt::new(1, 11);
        let delta_one_attempt = CheckpointAttempt::new(2, 12);
        let delta_two_attempt = CheckpointAttempt::new(3, 13);

        write_and_seal_partial(
            &backend,
            full_attempt,
            crate::vnode_partial::VnodePartial {
                operators: vec![("agg".into(), b"full".to_vec())],
                base: None,
                deltas: Vec::new(),
            },
        )
        .await;
        write_and_seal_partial(
            &backend,
            delta_one_attempt,
            crate::vnode_partial::VnodePartial {
                operators: Vec::new(),
                base: Some(full_attempt),
                deltas: vec![("agg".into(), b"delta-1".to_vec())],
            },
        )
        .await;
        write_and_seal_partial(
            &backend,
            delta_two_attempt,
            crate::vnode_partial::VnodePartial {
                operators: Vec::new(),
                base: Some(delta_one_attempt),
                deltas: vec![("agg".into(), b"delta-2".to_vec())],
            },
        )
        .await;

        let report = VnodeRehydrator::new(&backend)
            .rehydrate_at(&[0], delta_two_attempt)
            .await
            .unwrap();
        let decoded: Vec<_> = report.restored[&0]
            .iter()
            .map(|bytes| crate::vnode_partial::VnodePartial::decode(bytes).unwrap())
            .collect();
        assert_eq!(decoded.len(), 3);
        let (base, deltas) = resolve_op_chain(&decoded, "agg").expect("resolved aggregate chain");
        assert_eq!(base, b"full");
        assert_eq!(deltas, vec![b"delta-1".as_slice(), b"delta-2".as_slice()]);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn retention_keeps_fallback_delta_ancestors_until_rebase() {
        let backend = InProcessBackend::new(1);
        let attempts = [
            CheckpointAttempt::new(1, 11),
            CheckpointAttempt::new(2, 12),
            CheckpointAttempt::new(3, 13),
            CheckpointAttempt::new(4, 14),
            CheckpointAttempt::new(5, 15),
        ];

        write_and_seal_partial(
            &backend,
            attempts[0],
            crate::vnode_partial::VnodePartial {
                operators: vec![("agg".into(), b"full-1".to_vec())],
                base: None,
                deltas: Vec::new(),
            },
        )
        .await;
        for (attempt, parent, delta) in [
            (attempts[1], attempts[0], b"delta-2".as_slice()),
            (attempts[2], attempts[1], b"delta-3".as_slice()),
        ] {
            write_and_seal_partial(
                &backend,
                attempt,
                crate::vnode_partial::VnodePartial {
                    operators: Vec::new(),
                    base: Some(parent),
                    deltas: vec![("agg".into(), delta.to_vec())],
                },
            )
            .await;
        }
        write_and_seal_partial(
            &backend,
            attempts[3],
            crate::vnode_partial::VnodePartial {
                operators: vec![("agg".into(), b"full-4".to_vec())],
                base: None,
                deltas: Vec::new(),
            },
        )
        .await;
        write_and_seal_partial(
            &backend,
            attempts[4],
            crate::vnode_partial::VnodePartial {
                operators: Vec::new(),
                base: Some(attempts[3]),
                deltas: vec![("agg".into(), b"delta-5".to_vec())],
            },
        )
        .await;

        // At E5 with R=3 and C=2, manifests E2/E3 are retained, so state GC must keep their E1
        // FULL ancestor (state horizon 0, not the manifest horizon 2).
        backend.prune_before(0).await.unwrap();
        for attempt in [attempts[1], attempts[2]] {
            VnodeRehydrator::new(&backend)
                .rehydrate_at(&[0], attempt)
                .await
                .expect("retained fallback cut must keep its FULL ancestor");
        }

        // Once the fallback window starts at the E4 FULL re-base, E1 can be removed. The current
        // E5 chain remains valid, while an older delta cut fails closed instead of starting empty.
        backend.prune_before(2).await.unwrap();
        VnodeRehydrator::new(&backend)
            .rehydrate_at(&[0], attempts[4])
            .await
            .expect("post-rebase chain must not depend on the old FULL");
        let error = VnodeRehydrator::new(&backend)
            .rehydrate_at(&[0], attempts[2])
            .await
            .expect_err("a missing delta ancestor must fail closed");
        assert!(error.to_string().contains("delta parent"), "{error}");
        assert!(
            error.to_string().contains("has no exact state seal"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn large_epoch_gap_rebase_keeps_the_earliest_fallback_chain() {
        let backend = InProcessBackend::new(1);
        let obsolete = CheckpointAttempt::new(1, 11);
        write_and_seal_partial(
            &backend,
            obsolete,
            crate::vnode_partial::VnodePartial {
                operators: vec![("agg".into(), b"obsolete-full".to_vec())],
                base: None,
                deltas: Vec::new(),
            },
        )
        .await;

        // An allocator high-watermark jump must re-base at E100. With R=3/C=2 and a current E105,
        // E102 is the earliest retained manifest and state GC may advance only to E100.
        let attempts: Vec<_> = (100..=105)
            .map(|epoch| CheckpointAttempt::new(epoch, epoch + 1_000))
            .collect();
        for index in [0_usize, 3] {
            write_and_seal_partial(
                &backend,
                attempts[index],
                crate::vnode_partial::VnodePartial {
                    operators: vec![(
                        "agg".into(),
                        format!("full-{}", attempts[index].epoch).into_bytes(),
                    )],
                    base: None,
                    deltas: Vec::new(),
                },
            )
            .await;
            for child in (index + 1)..=(index + 2) {
                write_and_seal_partial(
                    &backend,
                    attempts[child],
                    crate::vnode_partial::VnodePartial {
                        operators: Vec::new(),
                        base: Some(attempts[child - 1]),
                        deltas: vec![(
                            "agg".into(),
                            format!("delta-{}", attempts[child].epoch).into_bytes(),
                        )],
                    },
                )
                .await;
            }
        }

        backend.prune_before(100).await.unwrap();
        assert!(backend
            .checkpoint_seal_inventory(obsolete)
            .await
            .unwrap()
            .is_none());
        for attempt in [attempts[2], attempts[5]] {
            let report = VnodeRehydrator::new(&backend)
                .rehydrate_at(&[0], attempt)
                .await
                .expect("fallback and current chains must survive the large-gap rebase prune");
            assert_eq!(report.restored[&0].len(), 3);
        }
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn retention_keeps_reference_then_delta_ancestry() {
        let backend = InProcessBackend::new(1);
        let full = CheckpointAttempt::new(100, 1_100);
        let reference = CheckpointAttempt::new(102, 1_102);
        let delta_one = CheckpointAttempt::new(103, 1_103);
        let delta_two = CheckpointAttempt::new(104, 1_104);

        write_and_seal_partial(
            &backend,
            full,
            crate::vnode_partial::VnodePartial {
                operators: vec![("agg".into(), b"full-100".to_vec())],
                base: None,
                deltas: Vec::new(),
            },
        )
        .await;
        write_and_seal_partial(
            &backend,
            reference,
            crate::vnode_partial::VnodePartial {
                operators: Vec::new(),
                base: Some(full),
                deltas: Vec::new(),
            },
        )
        .await;
        write_and_seal_partial(
            &backend,
            delta_one,
            crate::vnode_partial::VnodePartial {
                operators: Vec::new(),
                base: Some(reference),
                deltas: vec![("agg".into(), b"delta-103".to_vec())],
            },
        )
        .await;
        write_and_seal_partial(
            &backend,
            delta_two,
            crate::vnode_partial::VnodePartial {
                operators: Vec::new(),
                base: Some(delta_one),
                deltas: vec![("agg".into(), b"delta-104".to_vec())],
            },
        )
        .await;

        // R=3 permits the E102 reference to point two epochs back; C=2 then permits two
        // consecutive deltas. The earliest retained E104 fallback therefore needs additive
        // state slack (R-1)+C=4, preserving E100.
        backend.prune_before(100).await.unwrap();
        let report = VnodeRehydrator::new(&backend)
            .rehydrate_at(&[0], delta_two)
            .await
            .expect("reference followed by deltas must retain its FULL root");
        assert_eq!(report.restored[&0].len(), 4);
        let decoded: Vec<_> = report.restored[&0]
            .iter()
            .map(|bytes| crate::vnode_partial::VnodePartial::decode(bytes).unwrap())
            .collect();
        let (base, deltas) = resolve_op_chain(&decoded, "agg").expect("resolved aggregate chain");
        assert_eq!(base, b"full-100");
        assert_eq!(
            deltas,
            vec![b"delta-103".as_slice(), b"delta-104".as_slice()]
        );
    }

    #[tokio::test]
    async fn rehydrate_rejects_reference_parent_without_its_own_seal() {
        let backend = InProcessBackend::new(4);
        let base_attempt = CheckpointAttempt::new(5, 1);
        let base = crate::vnode_partial::VnodePartial {
            operators: vec![("agg".into(), vec![1, 2, 3])],
            base: None,
            deltas: Vec::new(),
        };
        backend
            .write_partial(base_attempt, 0, 0, Bytes::from(base.encode().unwrap()))
            .await
            .unwrap();

        let head_attempt = CheckpointAttempt::new(6, 2);
        let reference = crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(base_attempt),
            deltas: Vec::new(),
        };
        backend
            .write_partial(head_attempt, 0, 0, Bytes::from(reference.encode().unwrap()))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(head_attempt, None, &[0], &[])
            .await
            .unwrap());

        let error = VnodeRehydrator::new(&backend)
            .rehydrate_at(&[0], head_attempt)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("reference base"));
        assert!(error.to_string().contains("has no exact state seal"));
    }

    #[tokio::test]
    async fn rehydrate_at_rejects_an_unsealed_attempt() {
        let backend = InProcessBackend::new(4);
        let error = VnodeRehydrator::new(&backend)
            .rehydrate_at(&[0, 1], CheckpointAttempt::new(1, 1))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("has no exact state seal"));
    }

    #[tokio::test]
    async fn rehydrate_empty_request_is_noop() {
        let backend = InProcessBackend::new(4);
        seal_epoch(&backend, 1, &[0], b"x").await;
        let report = VnodeRehydrator::new(&backend)
            .rehydrate_at(&[], CheckpointAttempt::new(1, 1))
            .await
            .unwrap();
        assert_eq!(report.attempt, None);
        assert!(report.restored.is_empty());
    }

    #[tokio::test]
    async fn rehydrate_over_object_store_backend() {
        use object_store::local::LocalFileSystem;
        use object_store::ObjectStore;

        let dir = tempfile::tempdir().unwrap();
        let store: std::sync::Arc<dyn ObjectStore> =
            std::sync::Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let backend = ObjectStoreBackend::node_durable(store, "node-0", 4);
        seal_epoch(&backend, 5, &[0, 1], b"durable").await;

        let report = VnodeRehydrator::new(&backend)
            .rehydrate_at(&[0, 1], CheckpointAttempt::new(5, 5))
            .await
            .unwrap();

        assert_eq!(report.attempt, Some(CheckpointAttempt::new(5, 5)));
        assert_eq!(report.restored_count(), 2);
        assert_eq!(operator_payload(&report, 1), b"durable");
    }
}
