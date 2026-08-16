use super::{
    AssignmentDrainTransition, CheckpointAssignmentAdoption, CheckpointAssignmentFence,
    CheckpointParticipant, MAX_CHECKPOINT_PARTICIPANTS,
};
use crate::checkpoint::{LeaderProof, LeaderProofOwner};
use crate::state::{MAX_KEY_GROUP_COUNT, PARTITIONING_ABI_VERSION};
use uuid::Uuid;

fn participant(node_id: u64, boot: u128) -> CheckpointParticipant {
    CheckpointParticipant {
        node_id,
        boot_incarnation: Uuid::from_u128(boot),
    }
}

fn leader(node_id: u64, boot: u128) -> LeaderProof {
    LeaderProof {
        owner: LeaderProofOwner {
            node_id,
            boot_id: Uuid::from_u128(boot),
            process_term: 4,
        },
        fencing_token: 9,
    }
}

#[test]
fn certificate_binds_map_version_and_process_roster() {
    let fence = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 2, 1, 9],
        vec![participant(1, 11), participant(2, 22), participant(9, 99)],
    )
    .unwrap();
    assert!(fence.is_canonical());
    assert!(fence.matches_owner_map(&[1, 2, 1, 9]));
    assert_eq!(fence.participant_ids(), [1, 2, 9]);

    let different_map =
        CheckpointAssignmentFence::from_owner_map(7, &[2, 1, 1, 9], fence.participants.clone())
            .unwrap();
    assert_ne!(fence.digest(), different_map.digest());

    let restarted = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 2, 1, 9],
        vec![participant(1, 111), participant(2, 22), participant(9, 99)],
    )
    .unwrap();
    assert_ne!(fence.digest(), restarted.digest());
}

#[test]
fn certificate_digest_binds_partitioning_abi() {
    let fence = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 2],
        vec![participant(1, 11), participant(2, 22)],
    )
    .unwrap();
    let mut wrong_abi = fence.clone();
    wrong_abi.partitioning_abi_version = PARTITIONING_ABI_VERSION + 1;

    assert_ne!(fence.digest(), wrong_abi.digest());
    assert_ne!(
        CheckpointAssignmentFence::owner_map_digest(2, &[1, 2]),
        CheckpointAssignmentFence::owner_map_digest_for_abi(
            PARTITIONING_ABI_VERSION + 1,
            2,
            &[1, 2]
        )
    );
    assert!(!wrong_abi.is_canonical());
    assert!(!wrong_abi.matches_owner_map(&[1, 2]));
}

#[test]
fn certificate_and_adoption_require_current_partitioning_abi() {
    let fence =
        CheckpointAssignmentFence::from_owner_map(7, &[1], vec![participant(1, 11)]).unwrap();
    let mut missing_fence = serde_json::to_value(&fence).unwrap();
    missing_fence
        .as_object_mut()
        .unwrap()
        .remove("partitioning_abi_version");
    assert!(serde_json::from_value::<CheckpointAssignmentFence>(missing_fence).is_err());

    let mut wrong_fence = fence.clone();
    wrong_fence.partitioning_abi_version = PARTITIONING_ABI_VERSION + 1;
    assert!(serde_json::from_value::<CheckpointAssignmentFence>(
        serde_json::to_value(wrong_fence).unwrap()
    )
    .is_err());

    let adoption = CheckpointAssignmentAdoption {
        participant: participant(1, 11),
        assignment_version: fence.assignment_version,
        partitioning_abi_version: PARTITIONING_ABI_VERSION,
        vnode_count: fence.vnode_count,
        assignment_digest: fence.assignment_digest,
        vnode_state_ready: true,
    };
    assert!(adoption.is_canonical());
    assert!(adoption.matches_fence(&fence));

    let mut missing_adoption = serde_json::to_value(&adoption).unwrap();
    missing_adoption
        .as_object_mut()
        .unwrap()
        .remove("partitioning_abi_version");
    assert!(serde_json::from_value::<CheckpointAssignmentAdoption>(missing_adoption).is_err());

    let mut missing_readiness = serde_json::to_value(&adoption).unwrap();
    missing_readiness
        .as_object_mut()
        .unwrap()
        .remove("vnode_state_ready");
    assert!(serde_json::from_value::<CheckpointAssignmentAdoption>(missing_readiness).is_err());

    let mut wrong_adoption = adoption;
    wrong_adoption.partitioning_abi_version = PARTITIONING_ABI_VERSION + 1;
    assert!(serde_json::from_value::<CheckpointAssignmentAdoption>(
        serde_json::to_value(wrong_adoption).unwrap()
    )
    .is_err());
}

#[test]
fn malformed_or_incomplete_certificates_fail_closed() {
    assert!(
        CheckpointAssignmentFence::from_owner_map(7, &[1, 2], vec![participant(1, 11)]).is_err()
    );
    assert!(CheckpointAssignmentFence::from_owner_map(
        7,
        &[1],
        vec![participant(1, 11), participant(1, 12)]
    )
    .is_err());
    assert!(CheckpointAssignmentFence::from_owner_map(0, &[1], vec![participant(1, 11)]).is_err());
    assert!(matches!(
        CheckpointAssignmentFence::from_owner_map(
            7,
            &[1],
            vec![participant(1, 11), participant(2, 22)]
        ),
        Err(message) if message.contains("exact vnode-owner set")
    ));
}

#[test]
fn certificate_rejects_more_than_the_partitioning_abi_limit() {
    let owner_count = usize::try_from(MAX_KEY_GROUP_COUNT).unwrap() + 1;
    let owners = vec![1; owner_count];

    assert!(matches!(
        CheckpointAssignmentFence::from_owner_map(7, &owners, vec![participant(1, 11)]),
        Err(message) if message.contains("key-group count")
    ));
}

#[test]
fn certificate_participant_limit_accepts_129_and_rejects_130() {
    let maximum = u64::try_from(MAX_CHECKPOINT_PARTICIPANTS).unwrap();
    let participants = (1..=maximum)
        .map(|node_id| participant(node_id, u128::from(node_id)))
        .collect();
    let owners = (1..=maximum).collect::<Vec<_>>();
    let fence = CheckpointAssignmentFence::from_owner_map(7, &owners, participants).unwrap();
    assert!(fence.is_canonical());
    assert_eq!(fence.participants.len(), MAX_CHECKPOINT_PARTICIPANTS);

    let oversized = (1..=maximum + 1)
        .map(|node_id| participant(node_id, u128::from(node_id)))
        .collect();
    let oversized_owners = (1..=maximum + 1).collect::<Vec<_>>();
    assert!(matches!(
        CheckpointAssignmentFence::from_owner_map(8, &oversized_owners, oversized),
        Err(message) if message.contains("maximum is 129")
    ));

    let mut forged = fence;
    forged
        .participants
        .push(participant(maximum + 1, u128::from(maximum + 1)));
    assert!(!forged.is_canonical());

    let encoded = serde_json::to_vec(&forged).unwrap();
    assert!(serde_json::from_slice::<CheckpointAssignmentFence>(&encoded).is_err());
}

#[test]
fn drain_transition_acks_predecessor_roster_and_accepts_target_only_leader() {
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 2],
        vec![participant(1, 11), participant(2, 22)],
    )
    .unwrap();
    let target = CheckpointAssignmentFence::from_owner_map(
        8,
        &[2, 3],
        vec![participant(2, 22), participant(3, 33)],
    )
    .unwrap();
    let transition = AssignmentDrainTransition::new(predecessor, target, leader(3, 33)).unwrap();

    assert_eq!(
        transition
            .required_participants()
            .iter()
            .map(|participant| participant.node_id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(transition.id().is_canonical());
    assert_ne!(transition.digest(), [0; 32]);
}

#[test]
fn drain_transition_accepts_predecessor_only_leader() {
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 2],
        vec![participant(1, 11), participant(2, 22)],
    )
    .unwrap();
    let target = CheckpointAssignmentFence::from_owner_map(
        8,
        &[2, 3],
        vec![participant(2, 22), participant(3, 33)],
    )
    .unwrap();
    assert!(AssignmentDrainTransition::new(predecessor, target, leader(1, 11)).is_ok());
}

#[test]
fn drain_identity_rejects_version_overflow() {
    let identity = super::AssignmentDrainId {
        predecessor_version: u64::MAX,
        target_version: u64::MAX,
        digest: [1; 32],
    };
    assert!(!identity.is_canonical());
}
