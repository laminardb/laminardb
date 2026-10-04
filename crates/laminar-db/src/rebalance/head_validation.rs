//! Materialize and validate the durable assignment head before watcher publication.

use super::{AssignmentSnapshot, NodeId, SnapshotError, SnapshotWatcher};

pub(super) fn validate_local_assignment_head(
    current: &AssignmentSnapshot,
    current_owners: &[NodeId],
    local_version: u64,
    local_owners: &[NodeId],
) -> Result<(), String> {
    if current.version < local_version {
        return Err(format!(
            "durable assignment head {} regressed behind local assignment {local_version}",
            current.version
        ));
    }
    if current.version == local_version && current_owners != local_owners {
        return Err(format!(
            "durable and local assignment {} have different owner maps",
            current.version
        ));
    }
    Ok(())
}

impl SnapshotWatcher {
    pub(super) async fn load_materialized_assignment_head(
        &self,
    ) -> Result<Option<AssignmentSnapshot>, SnapshotError> {
        if let Some(controller) = self.controller.as_deref() {
            let authority = controller
                .checkpoint_authority()
                .map_err(|e| SnapshotError::Invalid(e.to_string()))?;
            authority
                .materialize_reserved_assignment_drain(&self.store)
                .await
                .map_err(assignment_publication_error)?;
        }
        self.store.load().await
    }
}

pub(super) fn assignment_publication_error(
    error: laminar_core::cluster::control::ClusterCheckpointAuthorityError,
) -> SnapshotError {
    use laminar_core::cluster::control::{ClusterCheckpointAuthorityError, LeaseError};
    match error {
        ClusterCheckpointAuthorityError::Authority(LeaseError::Io(reason)) => {
            SnapshotError::Io(reason)
        }
        ClusterCheckpointAuthorityError::Decision(
            laminar_core::checkpoint_decision::DecisionError::Io(reason),
        ) => SnapshotError::Io(reason),
        error => SnapshotError::Invalid(error.to_string()),
    }
}
