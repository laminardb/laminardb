use super::*;

fn is_assigned(state: &RebalanceState, topic: &str, partition: i32) -> bool {
    state
        .assigned_partitions()
        .contains(&(topic.to_string(), partition))
}

#[test]
fn test_assign() {
    let mut state = RebalanceState::new();
    state.on_assign(&[
        ("events".into(), 0),
        ("events".into(), 1),
        ("events".into(), 2),
    ]);

    assert_eq!(state.assigned_partitions().len(), 3);
    assert!(is_assigned(&state, "events", 0));
    assert!(is_assigned(&state, "events", 1));
    assert!(is_assigned(&state, "events", 2));
    assert!(!is_assigned(&state, "events", 3));
}

#[test]
fn test_revoke() {
    let mut state = RebalanceState::new();
    state.on_assign(&[("events".into(), 0), ("events".into(), 1)]);
    state.on_revoke(&[("events".into(), 1)]);

    assert_eq!(state.assigned_partitions().len(), 1);
    assert!(is_assigned(&state, "events", 0));
    assert!(!is_assigned(&state, "events", 1));
}

#[test]
fn test_eager_reassign() {
    let mut state = RebalanceState::new();
    state.on_assign(&[("events".into(), 0), ("events".into(), 1)]);
    // Eager rebalance: revoke all first, then assign new set
    state.on_revoke(&[("events".into(), 0), ("events".into(), 1)]);
    state.on_assign(&[("events".into(), 2), ("events".into(), 3)]);

    assert_eq!(state.assigned_partitions().len(), 2);
    assert!(!is_assigned(&state, "events", 0));
    assert!(is_assigned(&state, "events", 2));
}

#[test]
fn test_cooperative_assign() {
    let mut state = RebalanceState::new();
    state.on_assign(&[("events".into(), 0), ("events".into(), 1)]);
    // Cooperative: only revoke subset, assign new subset
    state.on_revoke(&[("events".into(), 1)]);
    state.on_assign(&[("events".into(), 2)]);

    assert_eq!(state.assigned_partitions().len(), 2);
    assert!(is_assigned(&state, "events", 0)); // retained
    assert!(!is_assigned(&state, "events", 1)); // revoked
    assert!(is_assigned(&state, "events", 2)); // newly assigned
}

#[test]
fn assignment_snapshot_is_stable_across_rebalance() {
    let mut state = RebalanceState::new();
    state.on_assign(&[("events".into(), 0), ("events".into(), 1)]);
    let pinned = state.assignment_snapshot();

    state.on_revoke(&[("events".into(), 0)]);
    assert!(pinned.contains(&("events".to_string(), 0)));
    assert!(!is_assigned(&state, "events", 0));
}

#[test]
fn test_empty_state() {
    let state = RebalanceState::new();
    assert_eq!(state.assigned_partitions().len(), 0);
    assert!(!is_assigned(&state, "events", 0));
}
