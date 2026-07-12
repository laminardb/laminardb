//! Checkpoint recovery: selects a committed manifest and resolves its operator-state sidecar.
//! Runtime owners restore sources, sinks, tables, and operators from that single recovered cut.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use laminar_core::checkpoint_decision::{CommitDecision, CommitDecisionScope};
use laminar_core::state::{CheckpointAttempt, StateBackend};
use laminar_core::storage::checkpoint_manifest::{
    CheckpointManifest, DurableCheckpointPhase, PipelineIdentity,
};
use laminar_core::storage::checkpoint_store::{CheckpointStore, CheckpointStoreError};
use laminar_core::storage::ValidationResult;
use tracing::{debug, error, info, warn};

use crate::error::DbError;

const VNODE_REHYDRATION_CONCURRENCY: usize = 32;

/// Result of a successful recovery from a checkpoint.
#[derive(Debug)]
pub struct RecoveredState {
    /// Manifest that was restored from.
    pub manifest: CheckpointManifest,
    decision: Option<CommitDecision>,
    #[cfg(feature = "cluster")]
    cluster_source_handoff: Option<HashMap<String, HashMap<String, String>>>,
}

impl RecoveredState {
    /// Returns the recovered epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.manifest.epoch
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn decision(&self) -> Option<&CommitDecision> {
        self.decision.as_ref()
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_cluster_source_handoff(
        &mut self,
        handoff: HashMap<String, HashMap<String, String>>,
    ) {
        self.cluster_source_handoff = Some(handoff);
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn cluster_source_handoff(
        &self,
    ) -> Option<&HashMap<String, HashMap<String, String>>> {
        self.cluster_source_handoff.as_ref()
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

/// Reads an exact decision-bound `partial.bin` chain for requested vnodes.
/// Applying the bytes is the caller's responsibility.
pub struct VnodeRehydrator<'a> {
    backend: &'a dyn StateBackend,
}

impl<'a> VnodeRehydrator<'a> {
    /// Create a rehydrator over `backend`.
    #[must_use]
    pub fn new(backend: &'a dyn StateBackend) -> Self {
        Self { backend }
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
        let sealed_vnodes: std::collections::HashSet<u32> =
            inventory.required_vnodes.into_iter().collect();
        if let Some(unsealed) = vnodes.iter().find(|vnode| !sealed_vnodes.contains(vnode)) {
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
            let Some(pbytes) = self
                .backend
                .read_partial(parent, vnode)
                .await
                .map_err(|e| {
                    DbError::Checkpoint(format!(
                        "[LDB-6051] failed to read vnode {vnode} delta parent {parent:?}: {e}"
                    ))
                })?
            else {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} delta parent {parent:?} is missing"
                )));
            };
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

        let Some(mut bytes) = self
            .backend
            .read_partial(attempt, vnode)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] failed to read vnode {vnode} at sealed epoch {} checkpoint {}: \
                     {error}",
                    attempt.epoch, attempt.checkpoint_id
                ))
            })?
        else {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] vnode {vnode} is missing at sealed epoch {} checkpoint {}",
                attempt.epoch, attempt.checkpoint_id
            )));
        };

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
            bytes = self
                .backend
                .read_partial(base, vnode)
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6051] failed to read vnode {vnode} reference base {base:?}: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6051] vnode {vnode} reference base {base:?} is missing"
                    ))
                })?;
            current = base;
        }
    }
}

/// One operator's resolved recovery chain: FULL base bytes + ordered `(changed, tombstones)` deltas.
#[cfg(feature = "cluster")]
pub(crate) type ResolvedOpChain<'a> = (&'a [u8], Vec<(&'a [u8], &'a [u8])>);

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
    let mut deltas: Vec<(&[u8], &[u8])> = Vec::new();
    for p in &chain[base_idx + 1..] {
        if let Some((_, d)) = p.deltas.iter().find(|(n, _)| n == op) {
            deltas.push((d.changed.as_slice(), d.tombstones_ipc.as_slice()));
        }
    }
    Some((base, deltas))
}

/// Loads the latest viable [`CheckpointManifest`] and resolves its external operator state.
pub struct RecoveryManager<'a> {
    store: &'a dyn CheckpointStore,
    expected_pipeline_identity: PipelineIdentity,
    expected_deployment_id: String,
    expected_decision_scope: CommitDecisionScope,
}

impl<'a> RecoveryManager<'a> {
    /// Create a recovery manager.
    #[must_use]
    pub fn new(store: &'a dyn CheckpointStore) -> Self {
        Self {
            store,
            expected_pipeline_identity: PipelineIdentity::empty(),
            expected_deployment_id: String::new(),
            expected_decision_scope: CommitDecisionScope::Local,
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

    /// Require durable decisions to belong to the active local or cluster recovery domain.
    #[must_use]
    pub(crate) fn with_decision_scope(mut self, scope: CommitDecisionScope) -> Self {
        self.expected_decision_scope = scope;
        self
    }

    fn ensure_peer_manifest_portable(manifest: &CheckpointManifest) -> Result<(), DbError> {
        let mut participant_local = Vec::new();
        if !manifest.operator_states.is_empty() || manifest.state_checksum.is_some() {
            participant_local.push("operator/materialized-view state");
        }
        if !manifest.table_offsets.is_empty() || manifest.table_store_checkpoint_path.is_some() {
            participant_local.push("reference-table state");
        }
        if manifest.watermark.is_some() || !manifest.source_watermarks.is_empty() {
            participant_local.push("local watermark state");
        }
        if participant_local.is_empty() {
            return Ok(());
        }
        Err(DbError::Checkpoint(format!(
            "[LDB-6041] checkpoint {} participant {} contains non-portable {}; a shed node \
             cannot bootstrap from another participant until that state is persisted in a \
             canonical cluster recovery capsule",
            manifest.checkpoint_id,
            manifest.participant_id,
            participant_local.join(", ")
        )))
    }

    async fn restore_decided(
        &self,
        decision: &laminar_core::checkpoint_decision::CommitDecision,
        decision_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    ) -> Result<Option<RecoveredState>, DbError> {
        if decision.scope != self.expected_decision_scope {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] durable decision scope {:?} does not match active runtime scope {:?}",
                decision.scope, self.expected_decision_scope
            )));
        }
        let local_participant = self.store.participant_id();
        if decision.scope == CommitDecisionScope::Local && local_participant != 0 {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] local decision recovery requires participant 0, but the checkpoint store is participant {local_participant}"
            )));
        }
        let storage_participant = if decision
            .participants
            .binary_search(&local_participant)
            .is_ok()
        {
            // An included participant must have persisted its own exact manifest. Missing or
            // corrupt local inventory is storage loss and remains fatal.
            local_participant
        } else {
            // A shed/new participant legitimately has no local manifest for this cut. The
            // decision binds the canonical peer namespace; shared vnode chains and source-offset
            // handoff remain authoritative for ownership-specific state.
            info!(
                local_participant,
                manifest_participant = decision.manifest_participant_id,
                epoch = decision.epoch,
                checkpoint_id = decision.checkpoint_id,
                assignment_version = decision.assignment_version,
                "cluster recovery: local participant absent from cut; using canonical peer manifest"
            );
            decision.manifest_participant_id
        };

        let manifest = self
            .store
            .load_manifest_for_participant(storage_participant, decision.checkpoint_id)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] decided epoch {} checkpoint {} participant {} manifest is unreadable: {error}",
                    decision.epoch, decision.checkpoint_id, storage_participant
                ))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] decided checkpoint {} is absent from participant \
                     {storage_participant} recovery inventory",
                    decision.checkpoint_id
                ))
            })?;
        if manifest.epoch != decision.epoch || manifest.checkpoint_id != decision.checkpoint_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] decision epoch {} checkpoint {} does not match participant \
                 {storage_participant} manifest epoch {} checkpoint {}",
                decision.epoch, decision.checkpoint_id, manifest.epoch, manifest.checkpoint_id
            )));
        }
        if storage_participant != local_participant {
            Self::ensure_peer_manifest_portable(&manifest)?;
        }
        self.try_restore(
            decision.checkpoint_id,
            manifest,
            Some(decision_store),
            storage_participant,
        )
        .await
    }

    async fn validate_empty_decision_genesis(&self) -> Result<(), DbError> {
        // Prepared-only inventory is normal residue from a crash before the first decision. A
        // Finalized manifest, torn pointer, or unreadable inventory is different: it is evidence
        // that authoritative decision history was lost, so genesis would replay visible output.
        let published = self.store.load_latest().await.map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6041] checkpoint recovery pointer is invalid while the durable decision inventory is empty: {error}"
            ))
        })?;
        if let Some(manifest) = published {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] published finalized checkpoint {} epoch {} exists but the durable decision inventory is empty",
                manifest.checkpoint_id, manifest.epoch
            )));
        }
        let checkpoint_ids = self.store.list_ids().await.map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6041] recovery inventory cannot be enumerated while the durable decision inventory is empty: {error}"
            ))
        })?;
        for checkpoint_id in checkpoint_ids {
            let manifest = self
                .store
                .load_by_id(checkpoint_id)
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6041] checkpoint {checkpoint_id} recovery inventory is unreadable while the durable decision inventory is empty: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6041] checkpoint {checkpoint_id} disappeared from recovery inventory while the durable decision inventory is empty"
                    ))
                })?;
            if manifest.checkpoint_id != checkpoint_id {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] storage checkpoint {checkpoint_id} contains manifest checkpoint {} while the durable decision inventory is empty",
                    manifest.checkpoint_id
                )));
            }
            if manifest.durable_phase == DurableCheckpointPhase::Finalized {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] finalized checkpoint {} epoch {} exists in recovery inventory but the durable decision inventory is empty",
                    manifest.checkpoint_id, manifest.epoch
                )));
            }
        }
        Ok(())
    }

    /// Recover from the latest committed, structurally valid checkpoint.
    ///
    /// Returns `Ok(None)` when no committed checkpoint exists. With a decision store, Prepared
    /// artifacts before the first decision are explicitly ignored as abandoned attempts.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the store fails or no stored checkpoint is usable.
    pub(crate) async fn recover(
        &self,
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<Option<RecoveredState>, DbError> {
        // A durable decision is the irrevocable recovery frontier. Once it exists, restoring any
        // older manifest can replay output already visible in an exact external sink. Resolve the
        // highest decision first and require an exact participant-bound manifest. Corruption or
        // storage loss for an included participant is fatal rather than a reason to rewind.
        if let Some(ds) = decision_store {
            let decision = ds
                .highest_committed()
                .await
                .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            if let Some(decision) = decision {
                return self.restore_decided(&decision, ds).await;
            }

            self.validate_empty_decision_genesis().await?;
            info!(
                "decision inventory is empty; ignoring any undecided Prepared artifacts and starting fresh"
            );
            return Ok(None);
        }
        let mut checkpoint_ids = self.store.list_ids().await.map_err(DbError::from)?;
        if checkpoint_ids.is_empty() {
            info!("checkpoint store is empty, starting fresh");
            return Ok(None);
        }
        checkpoint_ids.reverse();
        self.restore_first_for_participant(
            &checkpoint_ids,
            decision_store,
            self.store.participant_id(),
        )
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
        if let Some(ds) = decision_store {
            // Audit every write-ahead intent before selecting a target. Coordinated recovery's
            // quiesced cut is the highest retained decision; accepting genesis or an older target
            // would replay output already admitted at a newer durable frontier.
            let decisions = ds
                .recovery_decisions()
                .await
                .map_err(|e| DbError::Checkpoint(e.to_string()))?;
            return match decisions.last() {
                Some(decision) if decision.epoch == target_epoch => {
                    self.restore_decided(decision, ds).await
                }
                Some(decision) => Err(DbError::Checkpoint(format!(
                    "[LDB-6041] recovery target epoch {target_epoch} is not the highest durable frontier: epoch {} checkpoint {} is authoritative",
                    decision.epoch, decision.checkpoint_id
                ))),
                None if target_epoch == 0 => {
                    self.validate_empty_decision_genesis().await?;
                    Ok(None)
                }
                None => Err(DbError::Checkpoint(format!(
                    "[LDB-6041] recovery target epoch {target_epoch} has no commit decision"
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
        self.restore_first_for_participant(
            &candidate_ids,
            decision_store,
            self.store.participant_id(),
        )
        .await
    }

    /// Resolve external operator states from the sidecar file into inline entries.
    ///
    /// # Errors
    /// Returns a checkpoint error if the sidecar is unavailable, truncated, or unreadable.
    async fn resolve_external_states(
        &self,
        manifest: &mut CheckpointManifest,
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

        let state_data = match self.store.load_state_data(manifest.checkpoint_id).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6010] checkpoint {} sidecar is missing for external operators \
                     {external_ops:?}",
                    manifest.checkpoint_id
                )));
            }
            Err(e) => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6010] failed to load checkpoint {} sidecar: {e}",
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
    async fn restore_from(
        &self,
        mut manifest: CheckpointManifest,
    ) -> Result<RecoveredState, DbError> {
        self.resolve_external_states(&mut manifest).await?;

        info!(
            checkpoint_id = manifest.checkpoint_id,
            epoch = manifest.epoch,
            "recovering from checkpoint"
        );
        Ok(RecoveredState {
            manifest,
            decision: None,
            #[cfg(feature = "cluster")]
            cluster_source_handoff: None,
        })
    }

    /// Returns `true` if the checkpoint fails integrity validation.
    ///
    /// Operational validation failures propagate instead of being mislabeled as deterministic
    /// corruption and silently falling back while durable storage is unavailable.
    async fn is_checkpoint_corrupt(
        &self,
        storage_id: u64,
        manifest: &CheckpointManifest,
        storage_participant: u64,
    ) -> Result<bool, DbError> {
        if manifest.checkpoint_id != storage_id {
            error!(
                storage_id,
                manifest_id = manifest.checkpoint_id,
                "[LDB-6010] checkpoint identity mismatch"
            );
            return Ok(true);
        }
        let validation_errors = manifest.validate(self.store.vnode_count());
        if !validation_errors.is_empty() {
            error!(
                checkpoint_id = storage_id,
                issues = ?validation_errors,
                "[LDB-6010] checkpoint manifest is incompatible"
            );
            return Ok(true);
        }
        if storage_participant != self.store.participant_id() {
            // Peer bootstrap admits metadata-only manifests, so manifest validation above is the
            // complete integrity check; participant-local sidecars are deliberately unsupported.
            return Ok(false);
        }
        match self.store.validate_checkpoint(storage_id).await {
            Ok(ValidationResult {
                valid: false,
                ref issues,
                ..
            }) => {
                error!(
                    checkpoint_id = manifest.checkpoint_id,
                    issues = ?issues,
                    "[LDB-6010] checkpoint integrity check failed"
                );
                Ok(true)
            }
            Ok(_) => Ok(false),
            Err(e) => Err(DbError::from(e)),
        }
    }

    fn validate_decision_manifest_binding(
        &self,
        decision: &CommitDecision,
        storage_id: u64,
        manifest: &CheckpointManifest,
        storage_participant: u64,
    ) -> Result<(), DbError> {
        if decision.scope != self.expected_decision_scope {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] durable decision scope {:?} does not match active runtime scope {:?}",
                decision.scope, self.expected_decision_scope
            )));
        }
        if decision.checkpoint_id != storage_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] epoch {} commits checkpoint {}, but storage candidate is {storage_id}",
                manifest.epoch, decision.checkpoint_id
            )));
        }
        if decision.deployment_id != manifest.deployment_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] epoch {} checkpoint {storage_id} decision deployment '{}' does not match manifest deployment '{}'",
                manifest.epoch, decision.deployment_id, manifest.deployment_id
            )));
        }
        if decision
            .participants
            .binary_search(&storage_participant)
            .is_err()
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] epoch {} checkpoint {storage_id} storage participant {storage_participant} is absent from decision participants {:?}",
                manifest.epoch, decision.participants
            )));
        }
        let local_participant = self.store.participant_id();
        if storage_participant != local_participant
            && storage_participant != decision.manifest_participant_id
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] epoch {} checkpoint {storage_id} attempted peer manifest participant {storage_participant}, but the decision binds participant {}",
                manifest.epoch, decision.manifest_participant_id
            )));
        }
        Ok(())
    }

    /// Restore from `manifest` if it is viable; `None` means an older checkpoint may be tried
    /// because this candidate is deterministically corrupt or has no durable commit decision.
    async fn try_restore(
        &self,
        storage_id: u64,
        manifest: CheckpointManifest,
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
        storage_participant: u64,
    ) -> Result<Option<RecoveredState>, DbError> {
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
        let decision = match decision_store {
            Some(ds) => ds
                .decision(epoch)
                .await
                .map_err(|e| DbError::Checkpoint(e.to_string()))?,
            None => None,
        };
        if let Some(ref decision) = decision {
            self.validate_decision_manifest_binding(
                decision,
                storage_id,
                &manifest,
                storage_participant,
            )?;
        }

        // Prepared inventory is never a recovery cut on its own. When an exact decision store is
        // configured, it is authoritative for Finalized manifests too: the manifest phase is a
        // publication optimization, not a second commit oracle. At-least-once runtimes without a
        // decision store may recover only an integrity-valid Finalized manifest.
        if decision.is_none()
            && (manifest.durable_phase == DurableCheckpointPhase::Prepared
                || decision_store.is_some())
        {
            warn!(
                checkpoint_id,
                epoch,
                phase = ?manifest.durable_phase,
                "checkpoint has no exact commit decision; trying older"
            );
            return Ok(None);
        }

        if self
            .is_checkpoint_corrupt(storage_id, &manifest, storage_participant)
            .await?
        {
            warn!(
                checkpoint_id,
                epoch, "[LDB-6010] checkpoint corrupt, trying older"
            );
            if decision.is_some() {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] committed checkpoint {storage_id} is corrupt"
                )));
            }
            return Ok(None);
        }
        let mut state = self.restore_from(manifest).await?;
        state.decision = decision.clone();
        if state.manifest.durable_phase == DurableCheckpointPhase::Prepared {
            if storage_participant == self.store.participant_id() {
                self.store
                    .finalize(storage_id)
                    .await
                    .map_err(DbError::from)?;
            }
            // The exact decision is authoritative. A peer bootstrap does not rewrite another
            // participant's pointer, but the recovered in-memory cut is effectively finalized.
            state.manifest.durable_phase = DurableCheckpointPhase::Finalized;
        }
        Ok(Some(state))
    }

    /// Restore from the first viable checkpoint ID in try order.
    async fn restore_first_for_participant(
        &self,
        candidates: &[u64],
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
        storage_participant: u64,
    ) -> Result<Option<RecoveredState>, DbError> {
        for &checkpoint_id in candidates {
            match self
                .store
                .load_manifest_for_participant(storage_participant, checkpoint_id)
                .await
            {
                Ok(Some(manifest)) => {
                    if let Some(state) = self
                        .try_restore(checkpoint_id, manifest, decision_store, storage_participant)
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
    use laminar_core::storage::checkpoint_store::{
        FileSystemCheckpointStore, ObjectStoreCheckpointStore,
    };

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
    async fn recover_to_epoch_rejects_target_older_than_highest_durable_decision() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        decisions.record_committed(5, 1).await.unwrap();
        decisions.record_committed(7, 2).await.unwrap();

        let error = RecoveryManager::new(&store)
            .recover_to_epoch(5, Some(&decisions))
            .await
            .expect_err("an older target must not rewind the durable decision frontier");

        assert!(
            error.to_string().contains(
                "recovery target epoch 5 is not the highest durable frontier: epoch 7 checkpoint 2 is authoritative"
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
                "checkpoint recovery pointer is invalid while the durable decision inventory is empty"
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
    async fn irrevocable_highest_decision_never_falls_back_to_older_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());
        store.save(&finalized_manifest(1, 10)).await.unwrap();
        store.save(&finalized_manifest(2, 20)).await.unwrap();

        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        decisions.record_committed(20, 2).await.unwrap();
        let decided_manifest = dir
            .path()
            .join("checkpoints")
            .join("checkpoint_000002")
            .join("manifest.json");
        std::fs::write(decided_manifest, "corrupt").unwrap();

        let error = RecoveryManager::new(&store)
            .recover(Some(&decisions))
            .await
            .expect_err("a decided checkpoint cannot rewind to checkpoint 1");
        assert!(
            error.to_string().contains(
                "[LDB-6041] decided epoch 20 checkpoint 2 participant 0 manifest is unreadable"
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
    async fn prepared_manifest_with_empty_decision_inventory_recovers_genesis() {
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
    }

    #[tokio::test]
    async fn published_finalized_manifest_fails_with_empty_decision_inventory() {
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
                "published finalized checkpoint 1 epoch 1 exists but the durable decision inventory is empty"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn finalized_manifest_without_latest_pointer_fails_with_empty_decision_inventory() {
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
    async fn dangling_latest_pointer_fails_with_empty_decision_inventory() {
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
                "checkpoint recovery pointer is invalid while the durable decision inventory is empty"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn unreadable_manifest_fails_with_empty_decision_inventory() {
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
    async fn decided_prepared_manifest_is_finalized_and_recovered() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut prepared = CheckpointManifest::new(1, 7);
        prepared.deployment_id.clone_from(&deployment_id);
        store.save(&prepared).await.unwrap();
        decisions.record_committed(7, 1).await.unwrap();

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
    async fn finalized_manifest_requires_exact_decision_when_store_is_configured() {
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
    async fn exact_decision_must_match_manifest_deployment() {
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
        decisions.record_committed(1, 1).await.unwrap();

        let error = RecoveryManager::new(&store)
            .with_deployment_id(&manifest_deployment)
            .recover(Some(&decisions))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("decision deployment"), "{error}");
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
    async fn identity_mismatch_does_not_finalize_decided_prepared_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let mut prepared = CheckpointManifest::new(1, 7);
        prepared.pipeline_identity = pipeline_identity(0x33);
        store.save(&prepared).await.unwrap();

        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        decisions.record_committed(7, 1).await.unwrap();

        let error = RecoveryManager::new(&store)
            .with_pipeline_identity(&pipeline_identity(0x44))
            .recover(Some(&decisions))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("[LDB-6043]"));
        let persisted = store.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(persisted.durable_phase, DurableCheckpointPhase::Prepared);
    }

    #[tokio::test]
    async fn decision_scope_mismatch_does_not_finalize_prepared_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let prepared = CheckpointManifest::new(1, 7);
        store.save(&prepared).await.unwrap();

        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        decisions.record_committed(7, 1).await.unwrap();

        let error = RecoveryManager::new(&store)
            .with_decision_scope(CommitDecisionScope::Cluster)
            .recover(Some(&decisions))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("decision scope Local"));
        let persisted = store.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(persisted.durable_phase, DurableCheckpointPhase::Prepared);
    }

    #[tokio::test]
    async fn local_recovery_rejects_cluster_decision_before_manifest_access() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        decisions
            .record_committed_for_participants(7, 1, &[0], 0, 4)
            .await
            .unwrap();

        let error = RecoveryManager::new(&store)
            .recover(Some(&decisions))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("decision scope Cluster"));
        assert!(error.to_string().contains("active runtime scope Local"));
    }

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
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::clone(&backing),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

        let mut manifest = CheckpointManifest::new(7, 6);
        manifest.participant_id = 1;
        manifest.deployment_id.clone_from(&deployment_id);
        donor.save(&manifest).await.unwrap();
        decisions
            .record_committed_for_participants(6, 7, &[3, 1], 1, 4)
            .await
            .unwrap();

        let manager = RecoveryManager::new(&local)
            .with_deployment_id(&deployment_id)
            .with_decision_scope(CommitDecisionScope::Cluster);
        let recovered = manager
            .recover_to_epoch(6, Some(&decisions))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(recovered.manifest.participant_id, 1);
        assert_eq!(
            donor.load_by_id(7).await.unwrap().unwrap().durable_phase,
            DurableCheckpointPhase::Prepared,
            "peer bootstrap must not rewrite another participant's recovery pointer"
        );
    }

    #[tokio::test]
    async fn excluded_participant_rejects_peer_local_state() {
        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let donor =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/0/".into())
                .with_participant_id(0);
        let local =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
                .with_participant_id(2);
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::clone(&backing),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut manifest = CheckpointManifest::new(7, 6);
        manifest.participant_id = 0;
        manifest.deployment_id.clone_from(&deployment_id);
        manifest.source_watermarks.insert("events".into(), 42_000);
        donor.save(&manifest).await.unwrap();
        decisions
            .record_committed_for_participants(6, 7, &[0, 1], 0, 4)
            .await
            .unwrap();

        let error = RecoveryManager::new(&local)
            .with_deployment_id(&deployment_id)
            .with_decision_scope(CommitDecisionScope::Cluster)
            .recover(Some(&decisions))
            .await
            .expect_err("a donor-local watermark must not seed another participant");

        assert!(
            error.to_string().contains("local watermark state"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn decided_peer_manifest_must_match_the_exact_epoch() {
        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let donor =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
                .with_participant_id(1);
        let local =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
                .with_participant_id(2);
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
            std::sync::Arc::clone(&backing),
        );
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let mut wrong_epoch = CheckpointManifest::new(7, 5);
        wrong_epoch.participant_id = 1;
        wrong_epoch.deployment_id.clone_from(&deployment_id);
        donor.save(&wrong_epoch).await.unwrap();
        decisions
            .record_committed_for_participants(6, 7, &[1, 3], 1, 4)
            .await
            .unwrap();

        let error = RecoveryManager::new(&local)
            .with_deployment_id(&deployment_id)
            .with_decision_scope(CommitDecisionScope::Cluster)
            .recover_to_epoch(6, Some(&decisions))
            .await
            .expect_err("decision and donor manifest must identify one exact attempt");

        assert!(
            error.to_string().contains("decision epoch 6 checkpoint 7"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn included_participant_with_missing_exact_manifest_fails_closed() {
        let backing: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let local =
            ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
                .with_participant_id(2);
        let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(backing);
        decisions
            .record_committed_for_participants(6, 7, &[1, 2], 1, 4)
            .await
            .unwrap();

        let error = RecoveryManager::new(&local)
            .with_decision_scope(CommitDecisionScope::Cluster)
            .recover(Some(&decisions))
            .await
            .expect_err("an included participant cannot borrow a peer manifest");

        assert!(
            error
                .to_string()
                .contains("absent from participant 2 recovery inventory"),
            "{error}"
        );
    }
}

#[cfg(test)]
mod rehydration_tests {
    use super::*;
    use bytes::Bytes;
    use laminar_core::state::{InProcessBackend, ObjectStoreBackend};

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
            .seal_checkpoint(attempt, 0, vnodes, &[])
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
            .seal_checkpoint(full_attempt, 0, &[0], &[])
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
            .seal_checkpoint(reference_attempt, 0, &[0], &[])
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
