use std::collections::HashMap;

use bytes::Bytes;
use laminar_core::checkpoint::{
    AssignmentDrainTransition, CheckpointAssignmentFence, CheckpointParticipant, LeaderProof,
    LeaderProofOwner, PipelineIdentity,
};
use laminar_core::state::{CheckpointAttempt, NodeId};

use super::{PendingVnodeTransition, VnodeTransitionKind, VnodeTransitionOrigin};
use crate::checkpoint_coordinator::ValidatedClusterVnodeRestoreCut;
use crate::rebalance::AuditedCommittedDrainTransition;
use crate::recovery_manager::vnode_chains::LoadedVnodeChains;

fn participant(node_id: u64, incarnation: u128) -> CheckpointParticipant {
    CheckpointParticipant {
        node_id,
        boot_incarnation: uuid::Uuid::from_u128(incarnation),
    }
}

fn fence(
    version: u64,
    owners: &[u64],
    participants: Vec<CheckpointParticipant>,
) -> CheckpointAssignmentFence {
    CheckpointAssignmentFence::from_owner_map(version, owners, participants).unwrap()
}

fn loaded(attempt: CheckpointAttempt, vnodes: &[u32]) -> LoadedVnodeChains {
    LoadedVnodeChains {
        attempt: Some(attempt),
        chains: vnodes
            .iter()
            .map(|vnode| (*vnode, vec![Bytes::from_static(b"full")]))
            .collect::<HashMap<_, _>>(),
    }
}

#[test]
fn adjacent_assignment_derives_exact_local_rosters() {
    let local = participant(1, 1);
    let peer = participant(2, 2);
    let predecessor_owners = [NodeId(1), NodeId(1), NodeId(2), NodeId(2)];
    let target_owners = [NodeId(1), NodeId(2), NodeId(1), NodeId(2)];
    let predecessor = fence(4, &[1, 1, 2, 2], vec![local, peer]);
    let target = fence(5, &[1, 2, 1, 2], vec![local, peer]);
    let attempt = CheckpointAttempt::canonical(9);
    let identity = PipelineIdentity::empty();
    let cut = ValidatedClusterVnodeRestoreCut::synthetic_for_transition_test(
        attempt,
        identity.clone(),
        predecessor.clone(),
        &[1, 1, 2, 2],
        &[2],
    )
    .unwrap();

    let transition = PendingVnodeTransition::assignment_change(
        predecessor.clone(),
        &predecessor_owners,
        target.clone(),
        &target_owners,
        local,
        identity,
        Some(cut),
        loaded(attempt, &[2]),
        false,
        None,
    )
    .unwrap();

    assert_eq!(transition.acquired_vnodes(), &[2]);
    assert_eq!(transition.revoked_vnodes(), &[1]);
    assert_eq!(transition.target(), &target);
    assert!(matches!(
        transition.origin(),
        VnodeTransitionOrigin::AssignmentChange { predecessor: actual }
            if actual == &predecessor
    ));
    assert!(matches!(
        transition.kind(),
        VnodeTransitionKind::OwnershipChange
    ));
}

#[test]
fn replacement_incarnation_restores_target_without_revoking_old_process_state() {
    let old_local = participant(1, 1);
    let current_local = participant(1, 11);
    let peer = participant(2, 2);
    let predecessor_owners = [NodeId(1), NodeId(1), NodeId(2)];
    let target_owners = [NodeId(1), NodeId(2), NodeId(2)];
    let predecessor = fence(4, &[1, 1, 2], vec![old_local, peer]);
    let target = fence(5, &[1, 2, 2], vec![current_local, peer]);
    let attempt = CheckpointAttempt::canonical(9);
    let identity = PipelineIdentity::empty();
    let cut = ValidatedClusterVnodeRestoreCut::synthetic_for_transition_test(
        attempt,
        identity.clone(),
        predecessor.clone(),
        &[1, 1, 2],
        &[0],
    )
    .unwrap();

    let transition = PendingVnodeTransition::assignment_change(
        predecessor,
        &predecessor_owners,
        target,
        &target_owners,
        current_local,
        identity,
        Some(cut),
        loaded(attempt, &[0]),
        false,
        None,
    )
    .unwrap();

    assert_eq!(transition.acquired_vnodes(), &[0]);
    assert!(transition.revoked_vnodes().is_empty());
    assert!(matches!(
        transition.kind(),
        VnodeTransitionKind::OwnershipChange
    ));
}

#[test]
fn skipped_assignment_generation_is_unrepresentable() {
    let local = participant(1, 1);
    let owners = [NodeId(1), NodeId(2)];
    let predecessor = fence(4, &[1, 2], vec![local, participant(2, 2)]);
    let target = fence(6, &[1, 2], vec![local, participant(2, 2)]);

    let error = PendingVnodeTransition::assignment_change(
        predecessor,
        &owners,
        target,
        &owners,
        local,
        PipelineIdentity::empty(),
        None,
        LoadedVnodeChains::default(),
        false,
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("must be adjacent"), "{error}");
}

#[test]
fn assignment_version_overflow_is_not_adjacent() {
    let local = participant(1, 1);
    let owners = [NodeId(1)];
    let predecessor = fence(u64::MAX, &[1], vec![local]);
    let target = predecessor.clone();

    let error = PendingVnodeTransition::assignment_change(
        predecessor,
        &owners,
        target,
        &owners,
        local,
        PipelineIdentity::empty(),
        None,
        LoadedVnodeChains::default(),
        false,
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("must be adjacent"), "{error}");
}

#[test]
fn target_process_incarnation_must_match_the_local_process() {
    let local = participant(1, 1);
    let owners = [NodeId(1), NodeId(2)];
    let predecessor = fence(4, &[1, 2], vec![local, participant(2, 2)]);
    let target = fence(5, &[1, 2], vec![participant(1, 99), participant(2, 2)]);

    let error = PendingVnodeTransition::assignment_change(
        predecessor,
        &owners,
        target,
        &owners,
        local,
        PipelineIdentity::empty(),
        None,
        LoadedVnodeChains::default(),
        false,
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("process incarnation"), "{error}");
}

#[test]
fn full_local_restore_is_explicit_and_cut_bound() {
    let local = participant(1, 1);
    let peer = participant(2, 2);
    let owners = [NodeId(1), NodeId(2), NodeId(1)];
    let predecessor = fence(4, &[1, 2, 1], vec![local, peer]);
    let target = fence(5, &[1, 2, 1], vec![local, peer]);
    let attempt = CheckpointAttempt::canonical(9);
    let identity = PipelineIdentity::empty();
    let cut = ValidatedClusterVnodeRestoreCut::synthetic_for_transition_test(
        attempt,
        identity.clone(),
        predecessor.clone(),
        &[1, 2, 1],
        &[0, 2],
    )
    .unwrap();

    let transition = PendingVnodeTransition::assignment_change(
        predecessor,
        &owners,
        target,
        &owners,
        local,
        identity,
        Some(cut),
        loaded(attempt, &[0, 2]),
        true,
        None,
    )
    .unwrap();
    assert_eq!(transition.acquired_vnodes(), &[0, 2]);
    assert!(transition.revoked_vnodes().is_empty());
}

#[test]
fn live_assignment_change_rejects_a_cut_older_than_the_predecessor() {
    let local = participant(1, 1);
    let peer = participant(2, 2);
    let predecessor_owners = [NodeId(2), NodeId(2)];
    let target_owners = [NodeId(2), NodeId(1)];
    let older_cut_fence = fence(3, &[2, 2], vec![peer]);
    let predecessor = fence(4, &[2, 2], vec![peer]);
    let target = fence(5, &[2, 1], vec![local, peer]);
    let attempt = CheckpointAttempt::canonical(9);
    let identity = PipelineIdentity::empty();
    let cut = ValidatedClusterVnodeRestoreCut::synthetic_for_transition_test(
        attempt,
        identity.clone(),
        older_cut_fence,
        &[2, 2],
        &[1],
    )
    .unwrap();

    let error = PendingVnodeTransition::assignment_change(
        predecessor,
        &predecessor_owners,
        target,
        &target_owners,
        local,
        identity,
        Some(cut),
        loaded(attempt, &[1]),
        false,
        None,
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("exact predecessor certificate"),
        "{error}"
    );
}

#[test]
fn boot_recovery_accepts_an_older_cut_for_every_target_owned_vnode() {
    let local = participant(1, 1);
    let owners = [NodeId(1), NodeId(1)];
    let older_cut_fence = fence(3, &[1, 1], vec![local]);
    let target = fence(5, &[1, 1], vec![local]);
    let attempt = CheckpointAttempt::canonical(9);
    let identity = PipelineIdentity::empty();
    let cut = ValidatedClusterVnodeRestoreCut::synthetic_for_transition_test(
        attempt,
        identity.clone(),
        older_cut_fence,
        &[1, 1],
        &[0, 1],
    )
    .unwrap();

    let transition = PendingVnodeTransition::boot_recovery(
        target,
        &owners,
        local,
        identity,
        cut,
        loaded(attempt, &[0, 1]),
    )
    .unwrap();
    assert_eq!(transition.acquired_vnodes(), &[0, 1]);
    assert_eq!(
        transition
            .restores()
            .iter()
            .map(super::PendingVnodeRestore::vnode)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[test]
fn final_owner_exit_requires_the_exact_committed_transition() {
    let local = participant(1, 1);
    let successor = participant(2, 2);
    let predecessor = fence(4, &[1], vec![local]);
    let target = fence(5, &[2], vec![successor]);
    let transition = AssignmentDrainTransition::new(
        predecessor.clone(),
        target.clone(),
        LeaderProof {
            owner: LeaderProofOwner {
                node_id: local.node_id,
                boot_id: local.boot_incarnation,
                process_term: 1,
            },
            fencing_token: 1,
        },
    )
    .unwrap();
    let audited =
        AuditedCommittedDrainTransition::from_canonical_for_test(transition.clone()).unwrap();

    let pending = PendingVnodeTransition::assignment_change(
        predecessor,
        &[NodeId(1)],
        target,
        &[NodeId(2)],
        local,
        PipelineIdentity::empty(),
        None,
        LoadedVnodeChains::default(),
        false,
        Some(audited),
    )
    .unwrap();
    assert_eq!(pending.revoked_vnodes(), &[0]);
    assert!(matches!(
        pending.kind(),
        VnodeTransitionKind::CommittedFinalOwnerExit(actual) if actual == &transition
    ));
}
