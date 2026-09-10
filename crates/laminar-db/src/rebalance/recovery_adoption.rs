use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

use laminar_core::cluster::control::{
    AssignmentSnapshot, AssignmentSnapshotStore, ClusterController,
};
use laminar_core::state::VnodeRegistry;

use super::{abort_predecessor_checkpoint_for_recovery, try_suspend_recovery_assignment_authority};
use crate::db::{DbState, LaminarDB};

pub(super) async fn ensure_local_recovery_fault(
    db: &LaminarDB,
    controller: &ClusterController,
) -> Result<(), String> {
    controller.set_recovering(true);
    crate::coordinated_recovery::request_local_fault(controller, &db.pending_recovery_fault)
        .await
        .map(|_| ())
}

pub(super) fn require_recovery_cold_bootstrap(
    db: &LaminarDB,
    controller: &ClusterController,
    registry: &VnodeRegistry,
    snapshot: &AssignmentSnapshot,
) -> Result<(), String> {
    let target_owners = snapshot
        .to_vnode_vec(registry.vnode_count())
        .map_err(|error| error.to_string())?;
    let current_owners = registry.snapshot();
    let local = controller.instance_id();
    let acquires_vnodes = target_owners
        .iter()
        .zip(current_owners.iter())
        .any(|(target, current)| *target == local && *current != local);
    if !acquires_vnodes {
        return Ok(());
    }
    let state = DbState::load(&db.state);
    if matches!(state, DbState::Created | DbState::Faulted)
        && db.installed_vnode_state.lock().is_none()
    {
        return Ok(());
    }
    Err(format!(
        "recovery assignment {} vnode acquisition must wait for a faulted cold bootstrap; local graph is {state:?}",
        snapshot.version
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LocalRecoveryAssignmentScope {
    Participant,
    Ownerless,
}

/// Classify this exact process generation against an authority-audited recovery target.
///
/// An ownerless process may follow the target topology, but it has no checkpoint or recovery
/// authority and must not turn the target's recovery provenance into a new cluster-wide fault.
pub(super) fn local_recovery_assignment_scope(
    snapshot: &AssignmentSnapshot,
    controller: &ClusterController,
) -> Result<LocalRecoveryAssignmentScope, String> {
    let fence = snapshot
        .assignment_fence()
        .map_err(|error| error.to_string())?;
    let owners = snapshot
        .to_vnode_vec(fence.vnode_count)
        .map_err(|error| error.to_string())?;
    let owner_ids = owners.iter().map(|owner| owner.0).collect::<Vec<_>>();
    if !fence.is_canonical() || !fence.matches_owner_map(&owner_ids) {
        return Err(format!(
            "recovery assignment {} has no canonical owner-complete target fence",
            snapshot.version
        ));
    }

    let local_id = controller.instance_id().0;
    match fence.participant_incarnation(local_id) {
        Some(incarnation) if incarnation == controller.recovery_incarnation() => {
            Ok(LocalRecoveryAssignmentScope::Participant)
        }
        Some(_) => Err(format!(
            "recovery assignment {} certifies another incarnation of process {local_id}",
            snapshot.version
        )),
        None if owner_ids.contains(&local_id) => Err(format!(
            "recovery assignment {} gives process {local_id} ownership without checkpoint authority",
            snapshot.version
        )),
        None => Ok(LocalRecoveryAssignmentScope::Ownerless),
    }
}

pub(super) fn prepare_recovery_assignment_adoption<'a>(
    db: &'a Arc<LaminarDB>,
    store: &'a AssignmentSnapshotStore,
    controller: &'a ClusterController,
    registry: &'a VnodeRegistry,
    snapshot: &'a AssignmentSnapshot,
    deadline: tokio::time::Instant,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    Box::pin(async move {
        if !try_suspend_recovery_assignment_authority(db, controller, deadline).await? {
            return Err(format!(
                "recovery assignment {} waits for a local vnode transition",
                snapshot.version
            ));
        }
        if local_recovery_assignment_scope(snapshot, controller)?
            == LocalRecoveryAssignmentScope::Ownerless
        {
            return Ok(());
        }
        ensure_local_recovery_fault(db, controller).await?;
        abort_predecessor_checkpoint_for_recovery(store, controller, snapshot, deadline).await?;
        require_recovery_cold_bootstrap(db, controller, registry, snapshot)
    })
}

pub(super) async fn prepare_watched_recovery_adoption(
    db: &Arc<LaminarDB>,
    store: &AssignmentSnapshotStore,
    controller: &ClusterController,
    registry: &VnodeRegistry,
    snapshot: &AssignmentSnapshot,
    deadline: tokio::time::Instant,
) -> Result<u64, String> {
    ensure_local_recovery_fault(db, controller)
        .await
        .map_err(|error| format!("snapshot watcher: could not publish recovery fault: {error}"))?;
    abort_predecessor_checkpoint_for_recovery(store, controller, snapshot, deadline)
        .await
        .map_err(|error| {
            format!(
                "snapshot watcher: could not settle predecessor checkpoint for recovery: {error}"
            )
        })?;
    require_recovery_cold_bootstrap(db, controller, registry, snapshot).map_err(|error| {
        format!("snapshot watcher: recovery assignment waits for compute retirement: {error}")
    })?;
    Ok(db.assignment_authority_revision.load(Ordering::Acquire))
}

pub(super) async fn adopt_materialized_recovery_head(
    db: &Arc<LaminarDB>,
    store: &AssignmentSnapshotStore,
    controller: &ClusterController,
    registry: &VnodeRegistry,
    snapshot: &AssignmentSnapshot,
    deadline: tokio::time::Instant,
    operation_timeout: Duration,
) -> Result<u64, String> {
    prepare_recovery_assignment_adoption(db, store, controller, registry, snapshot, deadline)
        .await?;
    db.adopt_recovery_assignment_snapshot(snapshot.clone(), operation_timeout)
        .await
        .map_err(|error| error.to_string())?;
    let reconciled_version = registry.assignment_version();
    if reconciled_version < snapshot.version {
        return Err(format!(
            "durable recovery assignment {} was not adopted; local assignment remains {reconciled_version}",
            snapshot.version
        ));
    }
    Ok(reconciled_version)
}
