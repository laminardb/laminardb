//! Retained-state accounting of a SQL query operator: live aggregate state plus any staged or
//! displaced vnode-transition state awaiting publication or cleanup.

#[cfg(feature = "cluster")]
use super::SqlVnodeTransitionCleanup;
use super::{QueryState, SqlQueryOperator};
use crate::operator_graph::ManagedStateAccountingSnapshot;

impl SqlQueryOperator {
    pub(super) fn retained_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        let QueryState::Agg(aggregate) = &self.state else {
            return None;
        };

        #[cfg(feature = "cluster")]
        let (prepared_bytes, retired_bytes) = {
            let staged_transition = self
                .prepared_vnode_transition
                .as_ref()
                .map_or(0, |prepared| {
                    prepared
                        .aggregate
                        .accounted_state_bytes()
                        .saturating_add(prepared.topology.accounted_state_bytes())
                });
            let staged =
                staged_transition.saturating_add(self.prepared_subscription_output_bytes());
            match self.vnode_transition_cleanup.as_ref() {
                Some(SqlVnodeTransitionCleanup::Aborted(prepared)) => (
                    staged
                        .saturating_add(prepared.aggregate.accounted_state_bytes())
                        .saturating_add(prepared.topology.accounted_state_bytes()),
                    0,
                ),
                Some(SqlVnodeTransitionCleanup::Published {
                    aggregate,
                    topology,
                }) => (
                    staged,
                    aggregate
                        .accounted_state_bytes()
                        .saturating_add(topology.accounted_state_bytes()),
                ),
                None => (staged, 0),
            }
        };
        #[cfg(not(feature = "cluster"))]
        let (prepared_bytes, retired_bytes) = (0, 0);

        #[cfg(feature = "cluster")]
        let live = self.checked_live_state_bytes().unwrap_or(usize::MAX);
        #[cfg(not(feature = "cluster"))]
        let live = aggregate
            .accounted_state_bytes()
            .saturating_add(self.local_aggregate_output_cache_bytes());
        #[cfg(feature = "cluster")]
        let _ = aggregate;

        Some(ManagedStateAccountingSnapshot {
            live,
            prepared: prepared_bytes,
            retired: retired_bytes,
        })
    }
}
