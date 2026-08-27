use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Clear assignment-derived success before publishing an indeterminate graph generation.
pub(super) fn publish_cluster_execution_poison(
    poisoned: &AtomicBool,
    installed_vnode_state: Option<&crate::vnode_transition_staging::InstalledVnodeStateHandle>,
    pending_vnode_transition: Option<(
        &crate::vnode_transition_staging::PendingVnodeTransitionHandle,
        &Arc<crate::vnode_transition_staging::PendingVnodeTransition>,
    )>,
) {
    if let Some(installed_vnode_state) = installed_vnode_state {
        installed_vnode_state.lock().take();
    }
    if let Some((handle, expected)) = pending_vnode_transition {
        crate::vnode_transition_staging::retire_exact_pending_vnode_transition(handle, expected);
    }
    poisoned.store(true, Ordering::Release);
}
