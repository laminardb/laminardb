//! Preflight that defers a graceful assignment drain until its predecessor can deliver the cut.

use std::time::Duration;

use laminar_core::checkpoint::AssignmentDrainTransition;
use laminar_core::cluster::control::{ClusterController, PeriodicCheckpointHold};
use laminar_core::state::VnodeRegistry;
use tokio::time::Instant;
use tracing::debug;

use super::read_assignment_vnode_state_readiness;
use crate::checkpoint_coordinator::CheckpointCoordinator;
use crate::db::LaminarDB;

const SINK_EPOCH_POLL: Duration = Duration::from_millis(25);

/// Hold periodic checkpoints until no local checkpoint has consumed the committable sink epoch;
/// back-to-back periodic cuts can otherwise keep it consumed. Manual and recovery cuts still
/// proceed, so publication rechecks under the assignment-adoption lock.
pub(super) async fn hold_checkpoints_until_sink_epoch_opens<'c>(
    db: &LaminarDB,
    controller: &'c ClusterController,
    deadline: Instant,
) -> Option<PeriodicCheckpointHold<'c>> {
    if sink_epoch_admits_drain(db, deadline).await {
        return None;
    }
    let hold = controller.hold_periodic_checkpoints();
    while Instant::now() < deadline && !sink_epoch_admits_drain(db, deadline).await {
        tokio::time::sleep_until((Instant::now() + SINK_EPOCH_POLL).min(deadline)).await;
    }
    Some(hold)
}

async fn sink_epoch_admits_drain(db: &LaminarDB, deadline: Instant) -> bool {
    tokio::time::timeout_at(deadline, db.coordinator.lock())
        .await
        .is_ok_and(|coordinator| {
            coordinator
                .as_ref()
                .is_none_or(CheckpointCoordinator::sink_epoch_admits_drain)
        })
}

/// Whether the predecessor can deliver a drain cut now. `false` defers publication; the caller
/// holds the assignment-adoption lock, which also serializes checkpoint reservation.
pub(super) async fn predecessor_admits_drain(
    db: &LaminarDB,
    controller: &ClusterController,
    registry: &VnodeRegistry,
    version: u64,
    transition: &AssignmentDrainTransition,
    deadline: Instant,
) -> Result<bool, String> {
    let local_vnode_state_ready = tokio::time::timeout_at(
        deadline,
        db.local_vnode_state_is_ready(registry, &transition.predecessor),
    )
    .await
    .map_err(|_| {
        format!(
            "graceful rotation from assignment {version} timed out checking local vnode-state readiness"
        )
    })?
    .map_err(|error| error.to_string())?;
    if !local_vnode_state_ready {
        debug!(
            version,
            "graceful rotation deferred until local vnode state matches the predecessor"
        );
        return Ok(false);
    }
    let Some(published_fence) = controller.checkpoint_assignment_fence(version) else {
        debug!(
            version,
            "graceful rotation deferred until the predecessor assignment is adopted"
        );
        return Ok(false);
    };
    if published_fence != transition.predecessor {
        return Err(format!(
            "graceful rotation predecessor assignment {version} does not match the exact assignment-adoption certificate"
        ));
    }
    // This mutable report scan is a best-current preflight, not a cross-node lease. It
    // prevents assignment chaining before every participant's initial install; a later
    // withdrawal is contained by source drain and forced-checkpoint abort/recovery.
    if !read_assignment_vnode_state_readiness(controller, &transition.predecessor, deadline).await?
    {
        debug!(
            version,
            "graceful rotation deferred until every participant installs vnode state"
        );
        return Ok(false);
    }
    if !sink_epoch_admits_drain(db, deadline).await {
        debug!(
            version,
            "graceful rotation deferred until no checkpoint holds the committable sink epoch"
        );
        return Ok(false);
    }
    view_logs_admit_drain(db, transition, deadline).await?;
    Ok(true)
}

/// Refuse a rotation whose survivors could not adopt the leavers' append-view logs within their
/// share of the segment budget, judged from the newest committed cut. Refusing before the
/// drain avoids a committed drain whose target can never be published.
async fn view_logs_admit_drain(
    db: &LaminarDB,
    transition: &AssignmentDrainTransition,
    deadline: Instant,
) -> Result<(), String> {
    let departs = transition
        .predecessor
        .participant_ids()
        .into_iter()
        .any(|participant| !transition.target.contains(participant));
    let views = {
        let store = db.mv_store.read();
        (departs && store.has_append_views())
            .then(|| store.committed_views())
            .flatten()
    };
    let Some(views) = views else {
        return Ok(());
    };
    let manifests = tokio::time::timeout_at(deadline, views.latest_manifests())
        .await
        .map_err(|_| "graceful rotation timed out reading the committed view logs".to_owned())?
        .map_err(|error| error.to_string())?;
    match manifests {
        Some(manifests) => {
            laminar_core::checkpoint::check_view_log_adoption(&manifests, &transition.target)
                .map_err(|error| format!("graceful rotation refused: {error}"))
        }
        None => Ok(()),
    }
}
