use super::*;

#[test]
fn partitioning_key_group_count_rejects_every_out_of_range_value() {
    assert_eq!(KeyGroupCount::try_from(0_u16).unwrap_err().value(), 0);
    assert_eq!(KeyGroupCount::try_from(0_u32).unwrap_err().value(), 0);
    assert_eq!(
        KeyGroupCount::try_from(u32::from(u16::MAX) + 1)
            .unwrap_err()
            .value(),
        u32::from(u16::MAX) + 1
    );

    let one = KeyGroupCount::try_from(1_u16).unwrap();
    let max = KeyGroupCount::try_from(u32::from(u16::MAX)).unwrap();
    assert_eq!(u16::from(one), 1);
    assert_eq!(u32::from(max), u32::from(u16::MAX));
    assert_eq!(usize::from(max), usize::from(u16::MAX));
    assert_eq!(NonZeroU16::from(one), NonZeroU16::MIN);
}

#[test]
fn partitioning_abi_v1_raw_key_hash_golden_vectors() {
    assert_eq!(PARTITIONING_ABI_VERSION, 1);
    let actual = [
        key_hash(b""),
        key_hash(b"a"),
        key_hash(b"laminardb"),
        key_hash(&[0, 1, 0xff]),
        key_hash("key-☃".as_bytes()),
    ];
    assert_eq!(
        actual,
        [
            3_244_421_341_483_603_138,
            16_629_034_431_890_738_719,
            16_801_042_214_008_847_674,
            10_014_172_824_849_140_082,
            17_604_077_472_932_801_374,
        ]
    );
}

#[test]
fn rendezvous_placement_policy_golden_vector() {
    // Placement can evolve through an assignment transition; this vector
    // detects accidental churn in the current policy but is not ABI v1.
    let actual = rendezvous_assignment(12, &[NodeId(7), NodeId(3), NodeId(5)]);
    assert_eq!(
        actual.as_ref(),
        &[
            NodeId(5),
            NodeId(7),
            NodeId(3),
            NodeId(5),
            NodeId(5),
            NodeId(7),
            NodeId(5),
            NodeId(5),
            NodeId(5),
            NodeId(5),
            NodeId(5),
            NodeId(5),
        ]
    );
}

#[test]
fn new_registry_is_unassigned() {
    let r = VnodeRegistry::new(8);
    assert_eq!(r.vnode_count(), 8);
    for v in 0..8 {
        assert!(r.owner(v).is_unassigned());
    }
}

#[test]
fn single_owner_populates_all_slots() {
    let r = VnodeRegistry::single_owner(4, NodeId(42));
    for v in 0..4 {
        assert_eq!(r.owner(v), NodeId(42));
    }
}

#[test]
fn set_assignment_bumps_version() {
    let r = VnodeRegistry::new(4);
    let v0 = r.assignment_version();
    let new_assign: Arc<[NodeId]> = vec![NodeId(1), NodeId(2), NodeId(1), NodeId(2)].into();
    r.set_assignment(new_assign);
    assert!(r.assignment_version() > v0);
    assert_eq!(r.owner(0), NodeId(1));
    assert_eq!(r.owner(1), NodeId(2));
}

#[test]
fn vnode_for_key_in_range() {
    let r = VnodeRegistry::new(16);
    for i in 0..100 {
        let v = r.vnode_for_key(format!("k-{i}").as_bytes());
        assert!(v < 16);
    }
}

#[test]
#[should_panic(expected = "assignment length mismatch")]
fn set_assignment_rejects_wrong_length() {
    let r = VnodeRegistry::new(4);
    let bad: Arc<[NodeId]> = vec![NodeId(1)].into();
    r.set_assignment(bad);
}

#[test]
fn owner_out_of_range_returns_unassigned() {
    let r = VnodeRegistry::single_owner(4, NodeId(1));
    assert!(r.owner(10).is_unassigned());
}

#[test]
fn skipped_assignment_generation_forces_owner_reconciliation() {
    let r = VnodeRegistry::new_unassigned(1);
    let self_id = NodeId(7);
    r.set_assignment_and_version(vec![self_id].into(), 1);
    assert_eq!(r.versioned_snapshot().owner_changed_version(0), Some(1));

    r.set_assignment_and_version(vec![self_id].into(), 3);
    assert_eq!(
        r.versioned_snapshot().owner_changed_version(0),
        Some(3),
        "a missed intermediate generation may have transferred ownership"
    );
}

#[test]
#[should_panic(expected = "assignment version must advance")]
fn assignment_publication_rejects_equal_version_mutation() {
    let r = VnodeRegistry::new(1);
    r.set_assignment_and_version(vec![NodeId(9)].into(), 1);
}

#[test]
fn vnode_for_key_is_deterministic() {
    let r = VnodeRegistry::new(16);
    assert_eq!(r.vnode_for_key(b"key-x"), r.vnode_for_key(b"key-x"));
}

#[test]
fn owned_vnodes_filters_by_owner() {
    let r = VnodeRegistry::new(4);
    r.set_assignment(vec![NodeId(1), NodeId(2), NodeId(1), NodeId(2)].into());
    assert_eq!(owned_vnodes(&r, NodeId(1)), vec![0, 2]);
    assert_eq!(owned_vnodes(&r, NodeId(2)), vec![1, 3]);
    assert!(owned_vnodes(&r, NodeId(99)).is_empty());
}

#[test]
fn owned_vnodes_single_owner_returns_all() {
    let r = VnodeRegistry::single_owner(8, NodeId(42));
    assert_eq!(owned_vnodes(&r, NodeId(42)), (0..8).collect::<Vec<_>>());
}

#[test]
fn rendezvous_is_deterministic() {
    let peers = vec![NodeId(7), NodeId(3), NodeId(5)];
    let assignment = rendezvous_assignment(8, &peers);
    // Input order doesn't matter.
    let reversed = vec![NodeId(3), NodeId(5), NodeId(7)];
    assert_eq!(rendezvous_assignment(8, &reversed), assignment);
}

#[test]
fn rendezvous_single_peer_owns_everything() {
    let assignment = rendezvous_assignment(4, &[NodeId(99)]);
    assert!(assignment.iter().all(|&n| n == NodeId(99)));
}

#[test]
#[should_panic(expected = "needs at least one peer")]
fn rendezvous_rejects_empty_peer_list() {
    let _ = rendezvous_assignment(4, &[]);
}

#[test]
fn rendezvous_minimizes_state_movement() {
    let peers3 = vec![NodeId(1), NodeId(2), NodeId(3)];
    let peers4 = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)];

    let a3 = rendezvous_assignment(256, &peers3);
    let a4 = rendezvous_assignment(256, &peers4);

    let mut moved = 0;
    let mut moved_between_existing = 0;

    for v in 0..256usize {
        let o3 = a3[v];
        let o4 = a4[v];
        if o3 != o4 {
            moved += 1;
            if o4 != NodeId(4) {
                moved_between_existing += 1;
            }
        }
    }

    assert_eq!(
        moved_between_existing, 0,
        "No vnode should move between existing peers on a node join"
    );
    assert!(
        moved > 40 && moved < 90,
        "Expected roughly 25% of vnodes to move to the new peer, got {moved}"
    );

    for v in 0..256usize {
        if a3[v] != a4[v] {
            assert_eq!(a4[v], NodeId(4));
        }
    }
}

// -- Topology-aware placement --------------------------------------------

/// A node at (region, zone, rack).
fn node(id: u64, region: &str, zone: &str, rack: &str) -> (NodeId, Locality) {
    (
        NodeId(id),
        Locality::new(vec![region.into(), zone.into(), rack.into()]),
    )
}

const TIER_ZONE: usize = 1;

#[test]
fn locality_parse_and_domain_at() {
    let l = Locality::parse("region=us-east-1;zone=us-east-1a;rack=r17");
    assert_eq!(l.domain_at(0), "us-east-1");
    assert_eq!(l.domain_at(1), "us-east-1;us-east-1a");
    assert_eq!(l.domain_at(2), "us-east-1;us-east-1a;r17");
    assert_eq!(l.domain_at(99), "us-east-1;us-east-1a;r17"); // clamps to finest
    assert_eq!(Locality::parse("rack17").domain_at(0), "rack17"); // bare label
    assert_eq!(Locality::parse("").domain_at(0), ""); // unknown → empty domain
}

#[test]
fn owners_per_domain_counts_by_zone() {
    let nodes = vec![node(1, "r", "z1", "a"), node(2, "r", "z2", "a")];
    // z1 owns 2, z2 owns 1, and an unassigned owner folds into the empty domain.
    let owners = [NodeId(1), NodeId(1), NodeId(2), NodeId::UNASSIGNED];
    let counts = owners_per_domain(&owners, &nodes, TIER_ZONE);
    assert_eq!(counts["r;z1"], 2);
    assert_eq!(counts["r;z2"], 1);
    assert_eq!(counts[""], 1);
}
