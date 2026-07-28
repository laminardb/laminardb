//! Metadata-only validation of complete sealed vnode ancestry.

use std::{collections::HashMap, sync::Arc};

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

fn sealed_partial(
    inventory: &CheckpointSealInventory,
    vnode: u32,
) -> Result<SealedVnodePartial, DbError> {
    let index = inventory
        .sealed_partials
        .binary_search_by_key(&vnode, |partial| partial.vnode)
        .map_err(|_| {
            invalid_lineage(format!(
                "vnode {vnode} is absent from parent seal {:?}",
                inventory.attempt
            ))
        })?;
    Ok(inventory.sealed_partials[index].clone())
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
        let head_partial = sealed_partial(&head, vnode)?;
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
            let parent = sealed_partial(&parent_inventory, vnode)?;
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
