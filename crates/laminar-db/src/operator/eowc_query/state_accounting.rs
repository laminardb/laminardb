//! Retained-state accounting of an end-of-window operator whose window state is managed: live
//! state plus any staged or displaced vnode-transition state.

use super::EowcQueryOperator;
#[cfg(feature = "cluster")]
use super::{CoreWindowTransitionCleanup, PreparedEowcTransition};
use crate::core_window_state::CoreWindowState;
use crate::operator::capability::ManagedStateContract;
use crate::operator_graph::ManagedStateAccountingSnapshot;

impl EowcQueryOperator {
    pub(super) fn managed_window_state(&self) -> Option<&CoreWindowState> {
        if self.capability.managed_state != Some(ManagedStateContract::CoreWindowV1) {
            return None;
        }
        self.state.as_deref()
    }

    pub(super) fn retained_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        let window = self.managed_window_state()?;
        #[cfg(feature = "cluster")]
        let (prepared, retired) = {
            let staged = self
                .prepared_vnode_transition
                .as_ref()
                .map_or(0, PreparedEowcTransition::accounted_state_bytes);
            match self.vnode_transition_cleanup.as_ref() {
                Some(CoreWindowTransitionCleanup::Aborted(cleanup)) => {
                    (staged.saturating_add(cleanup.accounted_state_bytes()), 0)
                }
                Some(CoreWindowTransitionCleanup::Published(cleanup)) => {
                    (staged, cleanup.accounted_state_bytes())
                }
                None => (staged, 0),
            }
        };
        #[cfg(not(feature = "cluster"))]
        let (prepared, retired) = (0, 0);
        #[cfg(feature = "cluster")]
        let live = self.checked_live_state_bytes().unwrap_or(usize::MAX);
        #[cfg(not(feature = "cluster"))]
        let live = window.accounted_state_bytes();
        #[cfg(feature = "cluster")]
        let _ = window;
        Some(ManagedStateAccountingSnapshot {
            live,
            prepared,
            retired,
        })
    }
}
