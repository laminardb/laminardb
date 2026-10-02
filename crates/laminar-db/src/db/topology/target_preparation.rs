//! Publish target preparation only from a retained image and observed runtime owners.

use std::time::Duration;

use laminar_core::cluster::control::{TopologyAdmissionStatus, TopologyError};

use super::{DbError, LaminarDB, PreparedTopologyRestore};

impl LaminarDB {
    /// Certify this database's private exact-root target after observing parent retirement.
    /// Reuses the existing terminal lifecycle even on retry, then publishes one exact-process
    /// receipt through the configured controller. The caller cannot supply a termination flag,
    /// root, process identity or receipt. The target remains unstarted and the parent cut,
    /// namespace and catalog remain held. This does not commit or activate the target.
    ///
    /// The total cooperative deadline is 45 seconds. Unresolved runtime owners remain in the DB;
    /// the image stays owned by the caller. A cancelled/uncertain append may have succeeded:
    /// query operation status and retry this same image. Dropping an image never releases intake.
    /// The durable receipt is historical evidence, not a promise that this image is still resident.
    ///
    /// # Errors
    /// Rejects foreign/stale images, authority/recovery faults and unobserved termination.
    pub async fn certify_cluster_topology_target_preparation(
        &self,
        image: &mut PreparedTopologyRestore,
    ) -> Result<TopologyAdmissionStatus, DbError> {
        tokio::time::timeout(Duration::from_secs(45), async {
            self.retire_cluster_topology_parent(image).await?;
            let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
                TopologyError::Protocol(
                    "target preparation requires the configured controller".into(),
                )
            })?;
            let status = controller
                .certify_topology_target_preparation(&image.input)
                .await?;
            self.validate_topology_retirement_input(&image.input)
                .await?;
            Ok(status)
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }
}
