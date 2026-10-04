//! Original startup assertions and retired names come from the retained topology journal.

use std::collections::BTreeSet;

use super::super::{CatalogManifest, CatalogManifestRef, LeaderLeaseStore};
use super::CONTROL_TIMEOUT;
use crate::cluster::control::topology::{
    ClusterTopologyObjectTransition, TopologyAdmissionStatus, TopologyCatalogState, TopologyError,
};

impl LeaderLeaseStore {
    /// Load the complete original bootstrap after checking the current committed inventory.
    /// This is a startup assertion only; it never recreates objects removed by a migration.
    ///
    /// # Errors
    /// Rejects changed current authority, damaged adoption evidence or the bounded read deadline.
    pub async fn original_topology_catalog(
        &self,
        expected_current: &CatalogManifestRef,
    ) -> Result<Option<CatalogManifest>, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let Some((current, state)) = self.catalog_with_topology().await? else {
                return Err(TopologyError::Fenced);
            };
            if current.reference()? != *expected_current {
                return Err(TopologyError::Conflict(
                    "committed catalog changed during startup assertion".into(),
                ));
            }
            let TopologyCatalogState::Versioned {
                baseline,
                committed: Some(_),
            } = state
            else {
                return Ok(None);
            };
            Ok(Some(self.load_catalog_manifest(&baseline.manifest).await?))
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }

    /// Read names retired by committed migrations. The existing journal retains at most 64
    /// operations and forbids pruning them; a name therefore cannot silently return to generation
    /// one. A future incarnation contract must explicitly supersede this conservative rejection.
    ///
    /// # Errors
    /// Rejects corrupt retained evidence, a changed committed topology or the bounded deadline.
    pub async fn retired_topology_names(&self) -> Result<BTreeSet<String>, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let before = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            let names = self
                .retired_names_from_operations(&before.topology_operations)
                .await?;
            let after = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            if before.committed_topology_identity() != after.committed_topology_identity() {
                return Err(TopologyError::Conflict(
                    "committed topology changed while reading retired names".into(),
                ));
            }
            Ok(names)
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }

    pub(super) async fn retired_names_from_operations(
        &self,
        operations: &[TopologyAdmissionStatus],
    ) -> Result<BTreeSet<String>, TopologyError> {
        let mut names = BTreeSet::new();
        for operation in operations
            .iter()
            .filter(|operation| operation.commit.is_some())
        {
            self.audit_topology_operation(operation).await?;
            let plan = self.load_topology_plan(&operation.plan).await?;
            let descriptor = self
                .audit_topology_compatibility(&plan)
                .await?
                .ok_or_else(|| {
                    TopologyError::Protocol(
                        "committed topology has no compatibility mapping".into(),
                    )
                })?;
            names.extend(
                descriptor
                    .objects
                    .into_iter()
                    .filter(|object| object.transition == ClusterTopologyObjectTransition::Remove)
                    .map(|object| object.name),
            );
        }
        Ok(names)
    }
}
