use super::*;

#[test]
fn empty_has_no_leader() {
    assert_eq!(leader_of(&[]), None);
}

#[test]
fn only_unassigned_has_no_leader() {
    assert_eq!(leader_of(&[NodeId::UNASSIGNED]), None);
}

#[test]
fn single_instance_is_leader() {
    assert_eq!(leader_of(&[NodeId(42)]), Some(NodeId(42)));
}

#[test]
fn smallest_wins() {
    let live = [NodeId(100), NodeId(1), NodeId(50)];
    assert_eq!(leader_of(&live), Some(NodeId(1)));
}

#[test]
fn unassigned_ignored_with_real_ids() {
    let live = [NodeId::UNASSIGNED, NodeId(5), NodeId(3)];
    assert_eq!(leader_of(&live), Some(NodeId(3)));
}

#[test]
fn deterministic_across_calls() {
    let live = [NodeId(7), NodeId(3), NodeId(9)];
    assert_eq!(leader_of(&live), leader_of(&live));
}
