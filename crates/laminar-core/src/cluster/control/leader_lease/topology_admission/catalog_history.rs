//! Original startup assertions and retired names come from the retained topology journal.

use std::collections::{BTreeMap, BTreeSet};

use super::super::{CatalogManifest, CatalogManifestRef, LeaderLeaseStore};
use super::CONTROL_TIMEOUT;
use crate::cluster::control::topology::{
    ClusterTopologyObjectTransition, TopologyAdmissionStatus, TopologyCatalogState, TopologyError,
};

impl LeaderLeaseStore {
    /// Load the exact parent catalog of the current committed migration.
    ///
    /// # Errors
    /// Rejects changed current authority, invalid retained evidence or the bounded deadline.
    pub async fn parent_topology_catalog(
        &self,
        expected_current: &CatalogManifestRef,
    ) -> Result<Option<CatalogManifest>, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let Some((current, state)) = self.catalog_with_topology().await? else {
                return Err(TopologyError::Fenced);
            };
            if current.reference()? != *expected_current {
                return Err(TopologyError::Conflict(
                    "committed catalog changed during parent reconciliation".into(),
                ));
            }
            let TopologyCatalogState::Versioned {
                committed: Some(commit),
                ..
            } = state
            else {
                return Ok(None);
            };
            Ok(Some(
                self.load_catalog_manifest(&commit.parent_manifest).await?,
            ))
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }

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
    /// operations and forbids pruning them. Historical identities remain distinct after recreation.
    ///
    /// # Errors
    /// Rejects corrupt retained evidence, a changed committed topology or the bounded deadline.
    pub async fn retired_topology_names(&self) -> Result<BTreeSet<String>, TopologyError> {
        self.retired_topology_generations()
            .await
            .map(|generations| generations.into_keys().collect())
    }

    /// Read the latest retired incarnation of each name from the retained committed journal.
    ///
    /// # Errors
    /// Rejects corrupt retained evidence, changed committed authority or the bounded deadline.
    pub async fn retired_topology_generations(
        &self,
    ) -> Result<BTreeMap<String, u64>, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let before = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            let names = self
                .retired_generations_from_operations(&before.topology_operations)
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

    pub(super) async fn retired_generations_from_operations(
        &self,
        operations: &[TopologyAdmissionStatus],
    ) -> Result<BTreeMap<String, u64>, TopologyError> {
        let mut generations = BTreeMap::<String, u64>::new();
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
            for object in descriptor
                .objects
                .into_iter()
                .filter(|object| object.transition == ClusterTopologyObjectTransition::Remove)
            {
                let generation = generations.entry(object.name).or_default();
                *generation = (*generation).max(object.catalog_generation);
            }
        }
        Ok(generations)
    }
}

impl LeaderLeaseStore {
    pub(super) async fn validate_recreated_catalog_names(
        &self,
        plan: &crate::cluster::control::TopologyAdmissionPlan,
        parent: &CatalogManifest,
        target: &CatalogManifest,
        operations: &[TopologyAdmissionStatus],
    ) -> Result<(), TopologyError> {
        let retired = self.retired_generations_from_operations(operations).await?;
        if target.entries.iter().any(|entry| {
            !parent
                .entries
                .iter()
                .any(|old| old.canonical_name == entry.canonical_name)
                && retired.contains_key(&entry.canonical_name)
                && (plan.compatibility.is_none()
                    || retired
                        .get(&entry.canonical_name)
                        .and_then(|generation| generation.checked_add(1))
                        != Some(entry.catalog_generation))
        }) {
            return Err(TopologyError::Unsupported(
                "target reuses a retired name without its certified successor incarnation".into(),
            ));
        }
        Ok(())
    }
}
