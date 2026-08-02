//! Metadata-only validation of complete sealed vnode ancestry.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use laminar_core::{
    checkpoint::{VnodeRestoreContract, VnodeRestoreLimits},
    state::{
        CheckpointAttempt, CheckpointSealInventory, SealedVnodePartial, StateBackend,
        VnodePartialLineage,
    },
};

use crate::error::DbError;

/// Complete metadata proof for one sealed cluster vnode domain.
#[derive(Debug)]
pub(crate) struct ValidatedVnodeRestoreLineage {
    contract: VnodeRestoreContract,
    ancestor_attestations: Arc<ValidatedVnodeAncestorAttestations>,
}

impl ValidatedVnodeRestoreLineage {
    #[must_use]
    pub(crate) const fn contract(&self) -> &VnodeRestoreContract {
        &self.contract
    }

    #[must_use]
    pub(crate) fn into_ancestor_attestations(self) -> Arc<ValidatedVnodeAncestorAttestations> {
        self.ancestor_attestations
    }
}

#[derive(Debug)]
struct ValidatedVnodeSealAttempt {
    attempt: CheckpointAttempt,
    partials: Box<[SealedVnodePartial]>,
}

/// Exact vnode attestations retained after complete parent-seal validation.
#[derive(Debug)]
pub(crate) struct ValidatedVnodeAncestorAttestations {
    attempts: Box<[ValidatedVnodeSealAttempt]>,
    artifact_count: u64,
}

impl ValidatedVnodeAncestorAttestations {
    fn from_collected(
        collected: HashMap<CheckpointAttempt, Vec<SealedVnodePartial>>,
        expected_artifacts: u64,
    ) -> Result<Arc<Self>, DbError> {
        let mut artifact_count = 0_u64;
        let mut attempts = Vec::with_capacity(collected.len());
        for (attempt, mut partials) in collected {
            partials.sort_unstable_by_key(|partial| partial.vnode);
            if partials
                .windows(2)
                .any(|pair| pair[0].vnode == pair[1].vnode)
            {
                return Err(invalid_lineage(format!(
                    "seal {attempt:?} was visited more than once for one vnode"
                )));
            }
            artifact_count =
                artifact_count
                    .checked_add(u64::try_from(partials.len()).map_err(|_| {
                        invalid_lineage("retained attestation count does not fit u64")
                    })?)
                    .ok_or_else(|| invalid_lineage("retained attestation count overflow"))?;
            attempts.push(ValidatedVnodeSealAttempt {
                attempt,
                partials: partials.into_boxed_slice(),
            });
        }
        attempts.sort_unstable_by_key(|entry| (entry.attempt.epoch, entry.attempt.checkpoint_id));
        if artifact_count != expected_artifacts {
            return Err(invalid_lineage(format!(
                "retained {artifact_count} attestations for {expected_artifacts} verified artifacts"
            )));
        }
        Ok(Arc::new(Self {
            attempts: attempts.into_boxed_slice(),
            artifact_count,
        }))
    }

    #[cfg(test)]
    pub(crate) fn empty_for_test() -> Arc<Self> {
        Self::from_collected(HashMap::new(), 0).expect("empty ancestor proof is valid")
    }

    #[must_use]
    pub(crate) fn attestation(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Option<&SealedVnodePartial> {
        let attempt_index = self
            .attempts
            .binary_search_by_key(&(attempt.epoch, attempt.checkpoint_id), |entry| {
                (entry.attempt.epoch, entry.attempt.checkpoint_id)
            })
            .ok()?;
        let partials = &self.attempts[attempt_index].partials;
        let partial_index = partials
            .binary_search_by_key(&vnode, |partial| partial.vnode)
            .ok()?;
        Some(&partials[partial_index])
    }

    #[must_use]
    pub(crate) const fn artifact_count(&self) -> u64 {
        self.artifact_count
    }

    #[cfg(test)]
    pub(crate) fn attempt_count_for_test(&self) -> usize {
        self.attempts.len()
    }
}

fn invalid_lineage(message: impl Into<String>) -> DbError {
    DbError::Checkpoint(format!(
        "[LDB-6050] sealed vnode restore lineage is invalid: {}",
        message.into()
    ))
}

fn validate_inventory(
    inventory: &CheckpointSealInventory,
    expected_attempt: CheckpointAttempt,
) -> Result<(), DbError> {
    if inventory.attempt != expected_attempt {
        return Err(invalid_lineage(format!(
            "requested attempt {expected_attempt:?} returned seal {:?}",
            inventory.attempt
        )));
    }
    inventory
        .validate_vnode_partials()
        .map_err(|error| invalid_lineage(format!("seal {expected_attempt:?}: {error}")))
}

fn sealed_vnode_partial(
    inventory: &CheckpointSealInventory,
    vnode: u32,
) -> Result<&SealedVnodePartial, DbError> {
    let index = inventory
        .sealed_partials
        .binary_search_by_key(&vnode, |partial| partial.vnode)
        .map_err(|_| {
            invalid_lineage(format!(
                "vnode {vnode} is absent from parent seal {:?}",
                inventory.attempt
            ))
        })?;
    Ok(&inventory.sealed_partials[index])
}

fn checked_declared_totals(head: &CheckpointSealInventory) -> Result<(u64, u64), DbError> {
    head.sealed_partials
        .iter()
        .try_fold((0_u64, 0_u64), |(payload_bytes, artifacts), partial| {
            let payload_bytes = payload_bytes
                .checked_add(partial.lineage.total_payload_bytes())
                .ok_or_else(|| invalid_lineage("cluster lineage payload accounting overflow"))?;
            let artifacts = artifacts
                .checked_add(u64::from(partial.lineage.artifact_count()))
                .ok_or_else(|| invalid_lineage("cluster lineage artifact accounting overflow"))?;
            Ok((payload_bytes, artifacts))
        })
}

/// Build the bounded contract declared by the complete head inventory.
///
/// This validates the aggregate envelope but does not trust the transitive totals. Call
/// [`validate_vnode_restore_lineage`] before publishing or consuming the contract.
pub(crate) fn declared_vnode_restore_contract(
    head: &CheckpointSealInventory,
    limits: VnodeRestoreLimits,
) -> Result<VnodeRestoreContract, DbError> {
    validate_inventory(head, head.attempt)?;
    let vnode_count = head
        .assignment_fence
        .as_ref()
        .ok_or_else(|| invalid_lineage("cluster head has no assignment certificate"))?
        .vnode_count;
    let (payload_bytes, artifacts) = checked_declared_totals(head)?;
    VnodeRestoreContract::new(limits, payload_bytes, artifacts, vnode_count)
        .map_err(invalid_lineage)
}

async fn load_parent_inventory(
    backend: &dyn StateBackend,
    attempt: CheckpointAttempt,
) -> Result<CheckpointSealInventory, DbError> {
    let inventory = backend
        .checkpoint_seal_inventory_bounded(attempt)
        .await
        .map_err(|error| {
            invalid_lineage(format!("parent seal read failed for {attempt:?}: {error}"))
        })?
        .ok_or_else(|| invalid_lineage(format!("parent seal {attempt:?} is missing")))?;
    validate_inventory(&inventory, attempt)?;
    Ok(inventory)
}

#[derive(Debug)]
struct VnodeLineageAccounting {
    declared_payload_bytes: u64,
    declared_artifacts: u64,
    traversed_payload_bytes: u64,
    traversed_artifacts: u64,
    complete: bool,
}

#[derive(Debug)]
struct PendingLineageEdge {
    vnode: u32,
    child_attempt: CheckpointAttempt,
    child_payload_len: u64,
    child_lineage: VnodePartialLineage,
}

fn record_attestation(
    attempt: CheckpointAttempt,
    partial: &SealedVnodePartial,
    limits: &VnodeRestoreLimits,
    max_chain_artifacts: u64,
    accounting: &mut HashMap<u32, VnodeLineageAccounting>,
    pending: &mut BTreeMap<(u64, u64), Vec<PendingLineageEdge>>,
    retained: &mut HashMap<CheckpointAttempt, Vec<SealedVnodePartial>>,
    retain: bool,
) -> Result<(), DbError> {
    let vnode = partial.vnode;
    if partial.payload_len > limits.max_partial_payload_bytes {
        return Err(invalid_lineage(format!(
            "vnode {vnode} payload at {attempt:?} is {} bytes; maximum is {}",
            partial.payload_len, limits.max_partial_payload_bytes
        )));
    }
    let usage = accounting
        .get_mut(&vnode)
        .ok_or_else(|| invalid_lineage(format!("vnode {vnode} is outside the head roster")))?;
    if usage.complete {
        return Err(invalid_lineage(format!(
            "vnode {vnode} lineage continued after its root"
        )));
    }
    usage.traversed_payload_bytes = usage
        .traversed_payload_bytes
        .checked_add(partial.payload_len)
        .ok_or_else(|| invalid_lineage("vnode lineage payload accounting overflow"))?;
    usage.traversed_artifacts = usage
        .traversed_artifacts
        .checked_add(1)
        .ok_or_else(|| invalid_lineage("vnode lineage artifact accounting overflow"))?;
    if usage.traversed_artifacts > max_chain_artifacts {
        return Err(invalid_lineage(format!(
            "vnode {vnode} ancestry exceeds {max_chain_artifacts} artifacts"
        )));
    }
    if retain {
        retained.entry(attempt).or_default().push(partial.clone());
    }

    if let Some(parent) = partial.lineage.parent() {
        pending
            .entry((parent.epoch, parent.checkpoint_id))
            .or_default()
            .push(PendingLineageEdge {
                vnode,
                child_attempt: attempt,
                child_payload_len: partial.payload_len,
                child_lineage: partial.lineage,
            });
        return Ok(());
    }

    if VnodePartialLineage::root(partial.payload_len) != partial.lineage {
        return Err(invalid_lineage(format!(
            "vnode {vnode} at {attempt:?} does not terminate in an exact root"
        )));
    }
    if usage.traversed_payload_bytes != usage.declared_payload_bytes
        || usage.traversed_artifacts != usage.declared_artifacts
    {
        return Err(invalid_lineage(format!(
            "vnode {vnode} traversed {} bytes/{} artifacts but its head declares {} bytes/{} artifacts",
            usage.traversed_payload_bytes,
            usage.traversed_artifacts,
            usage.declared_payload_bytes,
            usage.declared_artifacts
        )));
    }
    usage.complete = true;
    Ok(())
}

/// Verify every required vnode's exact parent-seal lineage without reading any artifact body.
pub(crate) async fn validate_vnode_restore_lineage(
    backend: &dyn StateBackend,
    head: Arc<CheckpointSealInventory>,
    limits: VnodeRestoreLimits,
) -> Result<ValidatedVnodeRestoreLineage, DbError> {
    validate_inventory(&head, head.attempt)?;
    let vnode_count = head
        .assignment_fence
        .as_ref()
        .ok_or_else(|| invalid_lineage("cluster head has no assignment certificate"))?
        .vnode_count;
    limits.validate(vnode_count).map_err(invalid_lineage)?;

    let max_chain_artifacts = u64::from(limits.max_artifacts_per_vnode_chain);
    let mut accounting = HashMap::with_capacity(head.required_vnodes.len());
    let mut pending = BTreeMap::new();
    let mut retained = HashMap::new();

    for &vnode in &head.required_vnodes {
        let head_partial = sealed_vnode_partial(&head, vnode)?;
        let declared_payload_bytes = head_partial.lineage.total_payload_bytes();
        let declared_artifacts = u64::from(head_partial.lineage.artifact_count());
        if declared_artifacts > max_chain_artifacts {
            return Err(invalid_lineage(format!(
                "vnode {vnode} declares {declared_artifacts} artifacts; maximum is {max_chain_artifacts}"
            )));
        }

        if accounting
            .insert(
                vnode,
                VnodeLineageAccounting {
                    declared_payload_bytes,
                    declared_artifacts,
                    traversed_payload_bytes: 0,
                    traversed_artifacts: 0,
                    complete: false,
                },
            )
            .is_some()
        {
            return Err(invalid_lineage(format!(
                "head roster repeats vnode {vnode}"
            )));
        }
        record_attestation(
            head.attempt,
            head_partial,
            &limits,
            max_chain_artifacts,
            &mut accounting,
            &mut pending,
            &mut retained,
            false,
        )?;
    }

    while let Some(((epoch, checkpoint_id), mut edges)) = pending.pop_last() {
        let parent_attempt = CheckpointAttempt::new(epoch, checkpoint_id);
        edges.sort_unstable_by_key(|edge| edge.vnode);
        if let Some(duplicate) = edges.windows(2).find(|pair| pair[0].vnode == pair[1].vnode) {
            return Err(invalid_lineage(format!(
                "vnode {} reaches parent {parent_attempt:?} more than once",
                duplicate[0].vnode
            )));
        }
        let parent_inventory = load_parent_inventory(backend, parent_attempt).await?;
        for edge in edges {
            let parent = sealed_vnode_partial(&parent_inventory, edge.vnode)?;
            let expected = VnodePartialLineage::extend(
                edge.child_attempt,
                edge.child_payload_len,
                parent_attempt,
                parent.lineage,
            )
            .map_err(|error| {
                invalid_lineage(format!(
                    "vnode {} edge {:?} -> {parent_attempt:?}: {error}",
                    edge.vnode, edge.child_attempt
                ))
            })?;
            if edge.child_lineage != expected {
                return Err(invalid_lineage(format!(
                    "vnode {} at {:?} does not exactly extend parent {parent_attempt:?}",
                    edge.vnode, edge.child_attempt
                )));
            }
            record_attestation(
                parent_attempt,
                parent,
                &limits,
                max_chain_artifacts,
                &mut accounting,
                &mut pending,
                &mut retained,
                true,
            )?;
        }
    }

    let (cluster_payload_bytes, cluster_artifacts) = accounting.into_values().try_fold(
        (0_u64, 0_u64),
        |(payload_bytes, artifacts), usage| {
            if !usage.complete {
                return Err(invalid_lineage("vnode lineage did not terminate in a root"));
            }
            let payload_bytes = payload_bytes
                .checked_add(usage.traversed_payload_bytes)
                .ok_or_else(|| invalid_lineage("cluster lineage payload accounting overflow"))?;
            let artifacts = artifacts
                .checked_add(usage.traversed_artifacts)
                .ok_or_else(|| invalid_lineage("cluster lineage artifact accounting overflow"))?;
            Ok((payload_bytes, artifacts))
        },
    )?;

    let contract = VnodeRestoreContract::new(
        limits,
        cluster_payload_bytes,
        cluster_artifacts,
        vnode_count,
    )
    .map_err(invalid_lineage)?;
    let head_artifacts = u64::try_from(head.required_vnodes.len())
        .map_err(|_| invalid_lineage("head vnode count does not fit u64"))?;
    let ancestor_artifacts = cluster_artifacts
        .checked_sub(head_artifacts)
        .ok_or_else(|| invalid_lineage("cluster lineage has fewer artifacts than its head"))?;
    let ancestor_attestations =
        ValidatedVnodeAncestorAttestations::from_collected(retained, ancestor_artifacts)?;
    Ok(ValidatedVnodeRestoreLineage {
        contract,
        ancestor_attestations,
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use laminar_core::{
        checkpoint::{CheckpointAssignmentFence, CheckpointParticipant},
        state::{InProcessBackend, StateBackend},
    };

    use super::*;

    fn fence(vnode_count: u32) -> CheckpointAssignmentFence {
        CheckpointAssignmentFence::from_owner_map(
            7,
            &vec![1; usize::try_from(vnode_count).unwrap()],
            vec![CheckpointParticipant {
                node_id: 1,
                boot_incarnation: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
            }],
        )
        .unwrap()
    }

    async fn write_and_seal(
        backend: &InProcessBackend,
        fence: &CheckpointAssignmentFence,
        attempt: CheckpointAttempt,
        partials: &[(u32, VnodePartialLineage, &'static [u8])],
    ) -> Arc<CheckpointSealInventory> {
        for (vnode, lineage, payload) in partials {
            backend
                .write_certified_partial(
                    attempt,
                    *vnode,
                    fence,
                    1,
                    *lineage,
                    Bytes::from_static(payload),
                )
                .await
                .unwrap();
        }
        let vnodes = partials
            .iter()
            .map(|(vnode, _, _)| *vnode)
            .collect::<Vec<_>>();
        assert!(backend
            .seal_checkpoint(attempt, Some(fence), &vnodes, &[])
            .await
            .unwrap());
        Arc::new(
            backend
                .checkpoint_seal_inventory_bounded(attempt)
                .await
                .unwrap()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn complete_metadata_traversal_reproduces_exact_contract() {
        let backend = InProcessBackend::new(2);
        let fence = fence(2);
        let parent_attempt = CheckpointAttempt::canonical(1);
        let parent_payload = b"parent";
        let parent = write_and_seal(
            &backend,
            &fence,
            parent_attempt,
            &[
                (0, VnodePartialLineage::root(6), parent_payload),
                (1, VnodePartialLineage::root(6), parent_payload),
            ],
        )
        .await;
        let child_attempt = CheckpointAttempt::canonical(2);
        let child_payload = b"child";
        let child = write_and_seal(
            &backend,
            &fence,
            child_attempt,
            &[
                (
                    0,
                    VnodePartialLineage::extend(
                        child_attempt,
                        5,
                        parent_attempt,
                        parent.sealed_partials[0].lineage,
                    )
                    .unwrap(),
                    child_payload,
                ),
                (
                    1,
                    VnodePartialLineage::extend(
                        child_attempt,
                        5,
                        parent_attempt,
                        parent.sealed_partials[1].lineage,
                    )
                    .unwrap(),
                    child_payload,
                ),
            ],
        )
        .await;
        let limits = VnodeRestoreLimits::managed_vnode(11, 2, 2).unwrap();

        let validated = validate_vnode_restore_lineage(&backend, child, limits)
            .await
            .unwrap();

        assert_eq!(validated.contract().exact_cluster_lineage_payload_bytes, 22);
        assert_eq!(validated.contract().exact_cluster_lineage_artifacts, 4);
        assert_eq!(validated.ancestor_attestations.artifact_count(), 2);
        assert_eq!(validated.ancestor_attestations.attempt_count_for_test(), 1);
    }

    #[tokio::test]
    async fn forged_parent_arithmetic_fails_before_body_loading() {
        let backend = InProcessBackend::new(1);
        let fence = fence(1);
        let parent_attempt = CheckpointAttempt::canonical(1);
        write_and_seal(
            &backend,
            &fence,
            parent_attempt,
            &[(0, VnodePartialLineage::root(6), b"parent")],
        )
        .await;
        let child_attempt = CheckpointAttempt::canonical(2);
        let forged = VnodePartialLineage::extend(
            child_attempt,
            5,
            parent_attempt,
            VnodePartialLineage::root(7),
        )
        .unwrap();
        let child = write_and_seal(&backend, &fence, child_attempt, &[(0, forged, b"child")]).await;
        let limits = VnodeRestoreLimits::managed_vnode(12, 2, 1).unwrap();

        let error = validate_vnode_restore_lineage(&backend, child, limits)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("does not exactly extend parent"));
    }

    #[tokio::test]
    async fn missing_parent_seal_fails_closed() {
        let backend = InProcessBackend::new(1);
        let fence = fence(1);
        let parent_attempt = CheckpointAttempt::canonical(1);
        let child_attempt = CheckpointAttempt::canonical(2);
        let child_lineage = VnodePartialLineage::extend(
            child_attempt,
            5,
            parent_attempt,
            VnodePartialLineage::root(6),
        )
        .unwrap();
        let child = write_and_seal(
            &backend,
            &fence,
            child_attempt,
            &[(0, child_lineage, b"child")],
        )
        .await;
        let limits = VnodeRestoreLimits::managed_vnode(11, 2, 1).unwrap();

        let error = validate_vnode_restore_lineage(&backend, child, limits)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("parent seal"));
        assert!(error.to_string().contains("is missing"));
    }

    #[tokio::test]
    async fn declared_cluster_payload_max_plus_one_is_rejected() {
        let backend = InProcessBackend::new(1);
        let fence = fence(1);
        let attempt = CheckpointAttempt::canonical(1);
        let head = write_and_seal(
            &backend,
            &fence,
            attempt,
            &[(0, VnodePartialLineage::root(6), b"parent")],
        )
        .await;
        let limits = VnodeRestoreLimits::managed_vnode(5, 1, 1).unwrap();

        let error = declared_vnode_restore_contract(&head, limits).unwrap_err();

        assert!(error.to_string().contains("declares 6 payload bytes"));
        assert!(error.to_string().contains("maximum is 5"));
    }
}
