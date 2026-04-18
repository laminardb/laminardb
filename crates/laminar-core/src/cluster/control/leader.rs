//! Weak leader election: the lowest-ID live instance is the leader.
//!
//! No lease, no Raft, no CAS. The leader's only responsibilities are
//! triggering checkpoints and driving the assignment snapshot; both
//! tolerate a brief split-brain during phi-accrual failover because the
//! commit manifest (`epoch=N/_COMMIT`) is CAS-guarded. See the parent
//! design doc §7 for why this is enough.

use crate::cluster::discovery::NodeId;

/// Pick the leader from a set of live instance IDs.
///
/// Returns `None` if the set is empty or contains only the sentinel
/// `NodeId::UNASSIGNED`. Otherwise returns the smallest non-sentinel
/// ID.
#[must_use]
pub fn leader_of(live: &[NodeId]) -> Option<NodeId> {
    live.iter()
        .copied()
        .filter(|n| !n.is_unassigned())
        .min()
}

#[cfg(test)]
mod tests {
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
}
