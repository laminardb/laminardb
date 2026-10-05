//! Private target recovery reuses the existing compiler, checkpoint reader and operator codecs.

use laminar_core::checkpoint::merge_node_subscription_manifests;
use laminar_core::cluster::control::{TopologyError, TopologyOperationId, TopologyRestoreInput};

use super::restore::TopologyRestorePurpose;
use super::{DbError, LaminarDB, PreparedTopologyRestore};

impl LaminarDB {
    /// Build one unstarted recovery image from the greatest exact target checkpoint, or the
    /// explicitly mapped root when no target checkpoint has committed. State, source cursors and
    /// subscription frontiers come from the same selected cut. The existing compiler owns the
    /// image, and verified encoded state is released immediately after decoding.
    ///
    /// This changes no live catalog, transport, actors or output. A recovery image cannot use
    /// original migration installation/Release APIs; coordinated recovery authorization is required.
    ///
    /// # Errors
    /// Rejects live/faulted local actors, obsolete targets, changed ownership/process adoption,
    /// damaged state/output, missing source progress and configured read/state/deadline limits.
    pub async fn prepare_cluster_topology_recovery(
        &self,
        operation_id: TopologyOperationId,
    ) -> Result<PreparedTopologyRestore, DbError> {
        self.prepare_topology_restore_image(operation_id, TopologyRestorePurpose::Recovery)
            .await
    }
}

pub(super) fn validate_target_subscription_frontiers(
    graph: &crate::operator_graph::OperatorGraph,
    recovered: &crate::recovery_manager::RecoveredState,
    input: &TopologyRestoreInput,
) -> Result<(), DbError> {
    let views = recovered
        .manifests
        .iter()
        .filter_map(|manifest| {
            manifest
                .subscription_output
                .as_ref()
                .map(|subscription| (subscription, manifest.owned_vnodes.as_slice()))
        })
        .collect::<Vec<_>>();
    let merged = if views.is_empty() {
        Vec::new()
    } else {
        merge_node_subscription_manifests(
            recovered.committed.epoch,
            recovered.committed.checkpoint_id,
            recovered
                .committed
                .assignment_fence
                .as_ref()
                .ok_or(TopologyError::Fenced)?,
            &views,
        )
        .map_err(|error| TopologyError::Invalid(error.to_string()))?
    };
    let captures = graph.capture_subscription_frontiers()?;
    if captures.len() != merged.len() {
        return Err(TopologyError::Invalid(
            "target subscription roster differs from the selected checkpoint".into(),
        )
        .into());
    }
    for (capture, checkpoint) in captures.iter().zip(merged) {
        let expected = checkpoint
            .manifest
            .frontiers
            .iter()
            .filter(|frontier| {
                input
                    .owned_vnodes()
                    .binary_search(&u32::from(frontier.partition.get()))
                    .is_ok()
            })
            .collect::<Vec<_>>();
        if capture.certificate.as_ref() != &checkpoint.manifest.distribution_certificate
            || capture.frontiers.iter().collect::<Vec<_>>() != expected
        {
            return Err(TopologyError::Invalid("target subscription identity or exclusive sequence differs from the selected checkpoint".into()).into());
        }
    }
    Ok(())
}
