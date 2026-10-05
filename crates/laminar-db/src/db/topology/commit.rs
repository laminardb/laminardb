//! Commit owns no actors; private reconstruction is available before any target checkpoint.

use laminar_core::cluster::control::{TopologyAdmissionStatus, TopologyError};

use super::{DbError, LaminarDB, PreparedTopologyRestore};

impl LaminarDB {
    /// Irreversibly commit this DB's retained target image after complete protocol-four preparation.
    /// The configured leader supplies all authority/process/assignment fences. The target catalog
    /// and exact root share one append; target actors, output and intake stay fenced.
    ///
    /// The total cooperative budget is 45 seconds, including observed retirement and final reads.
    /// A cancelled/uncertain append can have committed. Retrying the same image resolves the same
    /// decision; after image loss use `recover_committed_cluster_topology`, never parent rollback.
    ///
    /// # Errors
    /// Rejects foreign/stale images, incomplete participant capabilities, local faults or unresolved
    /// authority. Commit alone grants no installation or output permission; runtime DDL remains
    /// guarded until automatic target recovery is certified.
    pub async fn commit_cluster_topology_target(
        &self,
        image: &mut PreparedTopologyRestore,
    ) -> Result<TopologyAdmissionStatus, DbError> {
        if !image.belongs_to(self) {
            return Err(TopologyError::Conflict(
                "target image belongs to a different database runtime".into(),
            )
            .into());
        }
        tokio::time::timeout(std::time::Duration::from_secs(45), async {
            let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
                TopologyError::Protocol("Commit requires the configured controller".into())
            })?;
            let status = self
                .cluster_topology_operation_status(image.input.operation().operation_id)
                .await?
                .ok_or_else(|| TopologyError::Conflict("unknown topology operation".into()))?;
            if status.commit.is_none() {
                self.certify_cluster_topology_target_preparation(image)
                    .await?;
                // The retained image's cursor observation may have aged while peers prepared.
                // Revalidate the sealed positions without resolving latest or starting readers.
                drop(
                    super::restore::prepare_source_positions(&image.candidate, &image.input)
                        .await?,
                );
                self.validate_topology_retirement_input(&image.input)
                    .await?;
                controller.commit_topology_target(&image.input).await?;
            }
            self.ensure_topology_restore_available(true)?;
            if !image.parent_retirement_observed {
                return Err(TopologyError::Fenced.into());
            }
            // Observe again even on an identical retry; a historical flag is not terminal proof.
            self.stop_pipeline_for_topology_retirement().await?;
            let fresh = controller
                .committed_topology_restore_input(image.input.operation().operation_id)
                .await?;
            if !fresh.same_restore_requirements(&image.input)
                && !fresh.is_committed_successor_of(&image.input)
            {
                return Err(TopologyError::Fenced.into());
            }
            self.ensure_topology_restore_available(true)?;
            let result = fresh.operation().clone();
            image.input = fresh;
            Ok(result)
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }
}
