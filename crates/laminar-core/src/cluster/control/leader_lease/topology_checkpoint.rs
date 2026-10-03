//! Durable authorization for the first target checkpoint's historical parent predecessor.

use super::topology_admission::CONTROL_TIMEOUT;
use super::{
    ClusterCheckpointAuthorityError, DecisionError, LeaderAuthorityRecord, LeaderLeaseStore,
};
use crate::checkpoint::CommittedCheckpointIndex;
use crate::cluster::control::{TopologyAdmissionPhase, TopologyError, TopologyMigrationRoot};

fn transition_error(error: TopologyError) -> ClusterCheckpointAuthorityError {
    match error {
        TopologyError::Authority(error) => error.into(),
        error => DecisionError::Conflict(error.to_string()).into(),
    }
}

impl LeaderLeaseStore {
    /// Validate an exact checkpoint chain edge. A pipeline transition requires the retained,
    /// audited topology Commit and participant-complete Release for that exact parent root.
    /// Returns its sealed mapping only for that transition; ordinary edges keep strict equality.
    /// This is artifact validation, never permission to install actors or open intake.
    ///
    /// # Errors
    /// Rejects unauthorized identity/inventory changes, corrupt/missing migration evidence and
    /// the 15 second read budget. No fingerprint mismatch override is exposed.
    pub async fn validate_cluster_checkpoint_predecessor(
        &self,
        target: &CommittedCheckpointIndex,
        parent: &CommittedCheckpointIndex,
    ) -> Result<Option<TopologyMigrationRoot>, ClusterCheckpointAuthorityError> {
        if target.pipeline_identity == parent.pipeline_identity {
            target
                .validate_predecessor_index(parent)
                .map_err(DecisionError::Conflict)?;
            return Ok(None);
        }
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let current = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            self.validate_checkpoint_predecessor_from(&current, target, parent)
                .await
        })
        .await
        .map_err(|_| {
            DecisionError::Conflict("topology checkpoint continuity audit timed out".into())
        })?
    }

    pub(super) async fn validate_checkpoint_predecessor_from(
        &self,
        current: &LeaderAuthorityRecord,
        target: &CommittedCheckpointIndex,
        parent: &CommittedCheckpointIndex,
    ) -> Result<Option<TopologyMigrationRoot>, ClusterCheckpointAuthorityError> {
        if target.pipeline_identity == parent.pipeline_identity {
            target
                .validate_predecessor_index(parent)
                .map_err(DecisionError::Conflict)?;
            return Ok(None);
        }
        let (_, parent_ref) = parent
            .encode_and_reference()
            .map_err(DecisionError::Conflict)?;
        let operation = current
            .topology_operations
            .iter()
            .find(|operation| {
                operation.phase == TopologyAdmissionPhase::Active
                    && operation.commit.is_some()
                    && operation
                        .cut
                        .as_ref()
                        .and_then(|cut| cut.committed.as_ref())
                        .is_some_and(|cut| cut.checkpoint == parent_ref)
            })
            .ok_or_else(|| {
                DecisionError::Conflict(
                    "checkpoint identity transition has no exact released topology root".into(),
                )
            })?;
        self.audit_topology_operation(operation).await?;
        let plan = self
            .load_topology_plan(&operation.plan)
            .await
            .map_err(transition_error)?;
        let descriptor = self
            .audit_topology_compatibility(&plan)
            .await
            .map_err(transition_error)?
            .ok_or_else(|| {
                DecisionError::Conflict("topology checkpoint transition has no descriptor".into())
            })?;
        let binding = operation.migration_root.as_ref().ok_or_else(|| {
            DecisionError::Conflict("topology checkpoint transition has no root".into())
        })?;
        let root = self
            .load_topology_root(&binding.root)
            .await
            .map_err(transition_error)?;
        if parent.assignment_fence.as_ref() != Some(&plan.assignment) {
            return Err(DecisionError::Conflict(
                "topology checkpoint predecessor differs from its frozen assignment".into(),
            )
            .into());
        }
        root.validate_target_checkpoint_predecessor(&descriptor, target, parent)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        Ok(Some(root))
    }
}
