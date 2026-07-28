//! Metadata-only validation of complete sealed vnode ancestry.

use std::{collections::HashMap, sync::Arc};

use laminar_core::{
    checkpoint::{VnodeRestoreContract, VnodeRestoreLimits},
    state::{CheckpointAttempt, CheckpointSealInventory, StateBackend, VnodePartialLineage},
};

use crate::error::DbError;

/// Complete metadata proof for one sealed cluster vnode domain.
#[derive(Debug)]
pub(crate) struct ValidatedVnodeRestoreLineage {
    contract: VnodeRestoreContract,
    inventories: HashMap<CheckpointAttempt, Arc<CheckpointSealInventory>>,
}

impl ValidatedVnodeRestoreLineage {
    #[must_use]
    pub(crate) const fn contract(&self) -> &VnodeRestoreContract {
        &self.contract
    }

    #[must_use]
    pub(crate) fn into_inventories(
        self,
    ) -> HashMap<CheckpointAttempt, Arc<CheckpointSealInventory>> {
        self.inventories
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

#[derive(Clone, Copy)]
struct SealedLineageStep {
    payload_len: u64,
    lineage: VnodePartialLineage,
}

fn sealed_lineage_step(
    inventory: &CheckpointSealInventory,
    vnode: u32,
) -> Result<SealedLineageStep, DbError> {
    let index = inventory
        .sealed_partials
        .binary_search_by_key(&vnode, |partial| partial.vnode)
        .map_err(|_| {
            invalid_lineage(format!(
                "vnode {vnode} is absent from parent seal {:?}",
                inventory.attempt
            ))
        })?;
    let partial = &inventory.sealed_partials[index];
    Ok(SealedLineageStep {
        payload_len: partial.payload_len,
        lineage: partial.lineage,
    })
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
    cache: &mut HashMap<CheckpointAttempt, Arc<CheckpointSealInventory>>,
    attempt: CheckpointAttempt,
) -> Result<Arc<CheckpointSealInventory>, DbError> {
    if let Some(inventory) = cache.get(&attempt) {
        return Ok(Arc::clone(inventory));
    }
    let inventory = backend
        .checkpoint_seal_inventory(attempt)
        .await
        .map_err(|error| {
            invalid_lineage(format!("parent seal read failed for {attempt:?}: {error}"))
        })?
        .ok_or_else(|| invalid_lineage(format!("parent seal {attempt:?} is missing")))?;
    validate_inventory(&inventory, attempt)?;
    let inventory = Arc::new(inventory);
    cache.insert(attempt, Arc::clone(&inventory));
    Ok(inventory)
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
    let mut inventories = HashMap::new();
    inventories.insert(head.attempt, Arc::clone(&head));
    let mut cluster_payload_bytes = 0_u64;
    let mut cluster_artifacts = 0_u64;

    for &vnode in &head.required_vnodes {
        let head_partial = sealed_lineage_step(&head, vnode)?;
        let declared_payload_bytes = head_partial.lineage.total_payload_bytes();
        let declared_artifacts = u64::from(head_partial.lineage.artifact_count());
        if declared_artifacts > max_chain_artifacts {
            return Err(invalid_lineage(format!(
                "vnode {vnode} declares {declared_artifacts} artifacts; maximum is {max_chain_artifacts}"
            )));
        }

        let mut current_attempt = head.attempt;
        let mut current = head_partial;
        let mut chain_payload_bytes = 0_u64;
        let mut chain_artifacts = 0_u64;
        loop {
            if current.payload_len > limits.max_partial_payload_bytes {
                return Err(invalid_lineage(format!(
                    "vnode {vnode} payload at {current_attempt:?} is {} bytes; maximum is {}",
                    current.payload_len, limits.max_partial_payload_bytes
                )));
            }
            chain_payload_bytes = chain_payload_bytes
                .checked_add(current.payload_len)
                .ok_or_else(|| invalid_lineage("vnode lineage payload accounting overflow"))?;
            chain_artifacts = chain_artifacts
                .checked_add(1)
                .ok_or_else(|| invalid_lineage("vnode lineage artifact accounting overflow"))?;
            if chain_artifacts > max_chain_artifacts {
                return Err(invalid_lineage(format!(
                    "vnode {vnode} ancestry exceeds {max_chain_artifacts} artifacts"
                )));
            }

            let Some(parent_attempt) = current.lineage.parent() else {
                VnodePartialLineage::root(current.payload_len)
                    .eq(&current.lineage)
                    .then_some(())
                    .ok_or_else(|| {
                        invalid_lineage(format!(
                            "vnode {vnode} at {current_attempt:?} does not terminate in an exact root"
                        ))
                    })?;
                break;
            };
            let parent_inventory =
                load_parent_inventory(backend, &mut inventories, parent_attempt).await?;
            let parent = sealed_lineage_step(&parent_inventory, vnode)?;
            let expected = VnodePartialLineage::extend(
                current_attempt,
                current.payload_len,
                parent_attempt,
                parent.lineage,
            )
            .map_err(|error| {
                invalid_lineage(format!(
                    "vnode {vnode} edge {current_attempt:?} -> {parent_attempt:?}: {error}"
                ))
            })?;
            if current.lineage != expected {
                return Err(invalid_lineage(format!(
                    "vnode {vnode} at {current_attempt:?} does not exactly extend parent {parent_attempt:?}"
                )));
            }
            current_attempt = parent_attempt;
            current = parent;
        }

        if chain_payload_bytes != declared_payload_bytes || chain_artifacts != declared_artifacts {
            return Err(invalid_lineage(format!(
                "vnode {vnode} traversed {chain_payload_bytes} bytes/{chain_artifacts} artifacts but its head declares {declared_payload_bytes} bytes/{declared_artifacts} artifacts"
            )));
        }
        cluster_payload_bytes = cluster_payload_bytes
            .checked_add(chain_payload_bytes)
            .ok_or_else(|| invalid_lineage("cluster lineage payload accounting overflow"))?;
        cluster_artifacts = cluster_artifacts
            .checked_add(chain_artifacts)
            .ok_or_else(|| invalid_lineage("cluster lineage artifact accounting overflow"))?;
    }

    let contract = VnodeRestoreContract::new(
        limits,
        cluster_payload_bytes,
        cluster_artifacts,
        vnode_count,
    )
    .map_err(invalid_lineage)?;
    Ok(ValidatedVnodeRestoreLineage {
        contract,
        inventories,
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
                boot_incarnation: uuid::Uuid::from_u128(1),
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
                .checkpoint_seal_inventory(attempt)
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
        let limits = VnodeRestoreLimits::global_singleton_compatibility(11, 2, 2).unwrap();

        let validated = validate_vnode_restore_lineage(&backend, child, limits)
            .await
            .unwrap();

        assert_eq!(validated.contract().exact_cluster_lineage_payload_bytes, 22);
        assert_eq!(validated.contract().exact_cluster_lineage_artifacts, 4);
        assert_eq!(validated.inventories.len(), 2);
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
        let limits = VnodeRestoreLimits::global_singleton_compatibility(12, 2, 1).unwrap();

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
        let limits = VnodeRestoreLimits::global_singleton_compatibility(11, 2, 1).unwrap();

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
        let limits = VnodeRestoreLimits::global_singleton_compatibility(5, 1, 1).unwrap();

        let error = declared_vnode_restore_contract(&head, limits).unwrap_err();

        assert!(error.to_string().contains("declares 6 payload bytes"));
        assert!(error.to_string().contains("maximum is 5"));
    }
}
