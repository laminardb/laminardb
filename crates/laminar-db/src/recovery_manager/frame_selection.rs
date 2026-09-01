//! Selects the exact state-frame inventory for local and reassigned recovery.
//!
//! The module owns assignment compatibility, donor authority, and graph-payload accounting. It
//! returns verified plans without reading checkpoint objects.

use super::{
    checkpoint_error, enforce_graph_payload_limit, graph_operator, predecessor_owner,
    selected_graph_payload_bytes, BTreeSet, CheckpointAssignmentFence, CheckpointManifest,
    ClusterRecoveryTarget, CommittedCheckpointIndex, DbError, NodeId, RecoveryFrameSelection,
    StateFrame, StateFrameKey, VerifiedStateFramePlan,
};

pub(super) fn select_local_state_frames(
    manifests: &[CheckpointManifest],
    local_participant: u64,
) -> Result<RecoveryFrameSelection, DbError> {
    let local_manifest = manifests
        .iter()
        .find(|manifest| manifest.participant_id == local_participant)
        .ok_or_else(|| {
            checkpoint_error(format!(
                "local participant {local_participant} is absent from the committed checkpoint"
            ))
        })?;
    Ok(RecoveryFrameSelection {
        plans: vec![VerifiedStateFramePlan::new(
            local_manifest,
            &local_manifest.state_frames,
        )?],
        reassigned: false,
        #[cfg(any(feature = "cluster", test))]
        predecessor_owners: Vec::new(),
        #[cfg(any(feature = "cluster", test))]
        target_vnodes: Vec::new(),
    })
}

pub(super) fn select_same_assignment_frames(
    target: &ClusterRecoveryTarget,
    predecessor: &CheckpointAssignmentFence,
    manifests: &[CheckpointManifest],
    local_participant: u64,
    predecessor_owners: Vec<NodeId>,
) -> Result<RecoveryFrameSelection, DbError> {
    #[cfg(not(any(feature = "cluster", test)))]
    drop(predecessor_owners);
    if target.assignment != *predecessor {
        return Err(checkpoint_error(
            "same-version recovery target differs from the committed assignment",
        ));
    }

    let local_manifest = manifests
        .iter()
        .find(|manifest| manifest.participant_id == local_participant);
    let plans = match (target.owned_vnodes.is_empty(), local_manifest) {
        (true, None) => Vec::new(),
        (false, Some(manifest))
            if manifest
                .owned_vnodes
                .iter()
                .copied()
                .map(u32::from)
                .eq(target.owned_vnodes.iter().copied()) =>
        {
            enforce_graph_payload_limit(
                selected_graph_payload_bytes(
                    &manifest.state_frames,
                    target.max_graph_payload_bytes,
                )?,
                target.max_graph_payload_bytes,
            )?;
            vec![VerifiedStateFramePlan::new(
                manifest,
                &manifest.state_frames,
            )?]
        }
        _ => {
            return Err(checkpoint_error(
                "local vnode roster does not match the committed assignment",
            ));
        }
    };

    Ok(RecoveryFrameSelection {
        plans,
        reassigned: false,
        #[cfg(any(feature = "cluster", test))]
        predecessor_owners,
        #[cfg(any(feature = "cluster", test))]
        target_vnodes: target.owned_vnodes.clone(),
    })
}

pub(super) fn validate_portable_reassignment(
    committed: &CommittedCheckpointIndex,
    target: &ClusterRecoveryTarget,
    predecessor: &CheckpointAssignmentFence,
) -> Result<(), DbError> {
    if predecessor.assignment_version >= target.assignment.assignment_version
        || predecessor.partitioning_abi_version != target.assignment.partitioning_abi_version
        || !committed.reassignment_portable
    {
        return Err(checkpoint_error(format!(
            "recovery target assignment {} is not a portable compatible newer assignment than committed assignment {}",
            target.assignment.assignment_version, predecessor.assignment_version
        )));
    }
    Ok(())
}

pub(super) fn select_reassigned_state_frames(
    target: &ClusterRecoveryTarget,
    manifests: &[CheckpointManifest],
    local_participant: u64,
    predecessor_owners: Vec<NodeId>,
) -> Result<RecoveryFrameSelection, DbError> {
    let donor_ids = target
        .owned_vnodes
        .iter()
        .map(|vnode| predecessor_owner(&predecessor_owners, *vnode).map(|owner| owner.0))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut plans = Vec::new();
    let mut graph_payload_bytes = 0usize;
    for manifest in manifests {
        let selected = select_manifest_state_frames(
            manifest,
            target,
            local_participant,
            &donor_ids,
            &predecessor_owners,
        )?;
        if selected.is_empty() {
            continue;
        }

        graph_payload_bytes = graph_payload_bytes
            .checked_add(selected_graph_payload_bytes(
                &selected,
                target.max_graph_payload_bytes,
            )?)
            .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                context: "checkpoint recovery graph payload".into(),
                accounted_bytes: usize::MAX,
                limit_bytes: target.max_graph_payload_bytes,
            })?;
        enforce_graph_payload_limit(graph_payload_bytes, target.max_graph_payload_bytes)?;
        plans.push(VerifiedStateFramePlan::new(manifest, &selected)?);
    }

    #[cfg(not(any(feature = "cluster", test)))]
    drop(predecessor_owners);

    Ok(RecoveryFrameSelection {
        plans,
        reassigned: true,
        #[cfg(any(feature = "cluster", test))]
        predecessor_owners,
        #[cfg(any(feature = "cluster", test))]
        target_vnodes: target.owned_vnodes.clone(),
    })
}

fn select_manifest_state_frames(
    manifest: &CheckpointManifest,
    target: &ClusterRecoveryTarget,
    local_participant: u64,
    donor_ids: &BTreeSet<u64>,
    predecessor_owners: &[NodeId],
) -> Result<Vec<StateFrame>, DbError> {
    let contributes_graph_state = donor_ids.contains(&manifest.participant_id);
    let is_local = manifest.participant_id == local_participant;
    let mut selected = Vec::new();
    for frame in &manifest.state_frames {
        match &frame.key {
            StateFrameKey::OperatorWhole { operator_id } => {
                let graph_state = graph_operator(operator_id)?;
                if (graph_state && contributes_graph_state) || (!graph_state && is_local) {
                    selected.push(frame.clone());
                }
            }
            StateFrameKey::Vnode { operator_id, vnode } => {
                if !graph_operator(operator_id)? {
                    return Err(checkpoint_error(format!(
                        "vnode state frame '{operator_id}' is not graph-managed"
                    )));
                }
                let vnode = u32::from(*vnode);
                if target.owned_vnodes.binary_search(&vnode).is_err() {
                    continue;
                }
                let expected_owner = predecessor_owner(predecessor_owners, vnode)?.0;
                if expected_owner != manifest.participant_id {
                    return Err(checkpoint_error(format!(
                        "vnode {vnode} state is declared by participant {}, expected {expected_owner}",
                        manifest.participant_id
                    )));
                }
                selected.push(frame.clone());
            }
        }
    }
    Ok(selected)
}
