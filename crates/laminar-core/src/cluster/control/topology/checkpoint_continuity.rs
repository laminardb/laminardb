//! The only explicit parent-to-target checkpoint continuity boundary.

use super::{
    ClusterTopologyObjectTransition, ClusterTopologyValidation, TopologyError,
    TopologyMigrationRoot,
};
use crate::checkpoint::{CheckpointScope, CommittedCheckpointIndex};
use crate::cluster::control::CatalogObjectKind;

impl TopologyMigrationRoot {
    /// Check the first target index against the exact retained parent index and sealed mapping.
    /// The authority must first audit this root's durable Commit/Release. This pure check grants
    /// no writer or runtime permission and never changes either checkpoint's historical identity.
    ///
    /// # Errors
    /// Rejects a foreign cut, changed source inventory, ownership/ABI/deployment or regressed
    /// watermarks. Ordinary checkpoint continuity continues to require identical pipeline IDs.
    pub fn validate_target_checkpoint_predecessor(
        &self,
        descriptor: &ClusterTopologyValidation,
        target: &CommittedCheckpointIndex,
        parent: &CommittedCheckpointIndex,
    ) -> Result<(), TopologyError> {
        target.validate().map_err(TopologyError::Invalid)?;
        parent.validate().map_err(TopologyError::Invalid)?;
        let (_, parent_ref) = parent
            .encode_and_reference()
            .map_err(TopologyError::Invalid)?;
        let parent_assignment = parent
            .assignment_fence
            .as_ref()
            .ok_or(TopologyError::Fenced)?;
        let target_assignment = target
            .assignment_fence
            .as_ref()
            .ok_or(TopologyError::Fenced)?;
        let sources = descriptor
            .objects
            .iter()
            .filter(|object| {
                object.kind == CatalogObjectKind::Source
                    && object.transition != ClusterTopologyObjectTransition::Remove
            })
            .map(|object| object.name.clone())
            .collect::<Vec<_>>();
        let parent_sources = descriptor
            .objects
            .iter()
            .filter(|object| {
                object.kind == CatalogObjectKind::Source
                    && object.transition != ClusterTopologyObjectTransition::AddFutureOnly
            })
            .map(|object| object.name.clone())
            .collect::<Vec<_>>();
        if self.cut.checkpoint != parent_ref
            || target.predecessor.as_ref() != Some(&parent_ref)
            || target.version < parent.version
            || target.epoch <= parent.epoch
            || parent.pipeline_identity != descriptor.parent_pipeline
            || target.pipeline_identity != descriptor.target_pipeline
            || parent.deployment_id != descriptor.deployment_id
            || target.deployment_id != descriptor.deployment_id
            || parent.scope != CheckpointScope::Cluster
            || target.scope != CheckpointScope::Cluster
            || parent.vnode_count != target.vnode_count
            || parent.source_names != parent_sources
            || target.source_names != sources
            || target_assignment.assignment_version < parent_assignment.assignment_version
            || target_assignment.assignment_digest != parent_assignment.assignment_digest
            || target_assignment.vnode_count != parent_assignment.vnode_count
            || target_assignment.partitioning_abi_version
                != parent_assignment.partitioning_abi_version
            || !target_assignment
                .participants
                .iter()
                .map(|participant| participant.node_id)
                .eq(parent_assignment
                    .participants
                    .iter()
                    .map(|participant| participant.node_id))
        {
            return Err(TopologyError::Invalid(
                "first target checkpoint does not continue the exact authorized migration root"
                    .into(),
            ));
        }
        let inherited_sources = self
            .preserved_objects
            .iter()
            .filter(|object| object.kind == CatalogObjectKind::Source)
            .map(|object| object.name.clone())
            .collect::<Vec<_>>();
        target
            .validate_source_watermark_continuity(parent, &inherited_sources)
            .map_err(TopologyError::Invalid)
    }
}
