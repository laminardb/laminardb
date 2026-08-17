//! Weak leader election: lowest-ID live instance wins. Split-brain is
//! tolerated because `epoch=N/_COMMIT` is CAS-guarded.

use crate::cluster::discovery::NodeId;

/// Smallest non-sentinel ID in `live`, or `None` if empty/all-sentinel.
#[must_use]
pub fn leader_of(live: &[NodeId]) -> Option<NodeId> {
    live.iter().copied().filter(|n| !n.is_unassigned()).min()
}

#[cfg(test)]
mod tests;
