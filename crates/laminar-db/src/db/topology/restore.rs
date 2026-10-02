//! Private target images reuse the isolated compiler and exact-cut recovery loader.

use std::collections::BTreeMap;
use std::sync::Arc;

use laminar_core::checkpoint::{
    CheckpointAttempt, CheckpointScope, ObjectStoreCheckpointStore, StateFrameKey,
};
use laminar_core::cluster::control::{
    TopologyError, TopologyMigrationRoot, TopologyOperationId, TopologyRestoreInput,
    TopologyVersion,
};

use super::{DbError, LaminarDB};

/// A source's preparation boundary. A sealed new-source cursor is never processing history.
#[derive(Debug)]
pub enum PreparedTopologySourcePosition {
    /// Existing committed progress, retaining the exact parent attempt and assignment metadata.
    Preserved {
        /// Definitive old checkpoint attempt.
        attempt: CheckpointAttempt,
        /// Existing connector replay cursor.
        checkpoint: laminar_connectors::checkpoint::SourceCheckpoint,
    },
    /// New global unowned position. Installation must validate/filter its current assignment;
    /// no checkpoint attempt or acknowledgement is invented here.
    Initialized {
        /// Concrete cursor sealed in the immutable root.
        checkpoint: laminar_connectors::checkpoint::SourceCheckpoint,
    },
}

/// One unstarted target state image. It owns the existing local compiler slot until dropped.
/// It has no source/sink actors or installation/output authority. Existing fenced parent transport
/// handles provide channel decoding context; no receiver or sender is started by preparation.
/// Even a successful preparation requires observed predecessor retirement, fresh authority checks, Commit and
/// coordinated target Release before it can become a runtime graph.
pub struct PreparedTopologyRestore {
    pub(crate) candidate: LaminarDB,
    pub(crate) graph: crate::operator_graph::OperatorGraph,
    pub(super) input: TopologyRestoreInput,
    recovered: crate::recovery_manager::RecoveredState,
    sources: BTreeMap<String, PreparedTopologySourcePosition>,
    restored_frames: usize,
    pub(super) parent_retirement_observed: bool,
    // Declared last: the private graph/catalog are dropped before another compiler can run.
    compiler: tokio::sync::OwnedMutexGuard<()>,
}

impl std::fmt::Debug for PreparedTopologyRestore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedTopologyRestore")
            .field("operation", &self.input.operation().operation_id)
            .field("target_version", &self.target_version())
            .field("restored_frames", &self.restored_frames)
            .field(
                "parent_retirement_observed",
                &self.parent_retirement_observed,
            )
            .field("managed_state_bytes", &self.managed_state_bytes())
            .field(
                "private_catalog_state",
                &super::DbState::load(&self.candidate.state),
            )
            .finish_non_exhaustive()
    }
}

impl PreparedTopologyRestore {
    /// Whether this image's parent actors were observed terminal after exact authority checks.
    /// This is a local observation, not a durable readiness receipt or target output permit.
    /// A future installer must revalidate current authority and the retired runtime boundary.
    #[must_use]
    pub const fn parent_retirement_observed(&self) -> bool {
        self.parent_retirement_observed
    }

    pub(super) fn belongs_to(&self, db: &LaminarDB) -> bool {
        Arc::ptr_eq(
            tokio::sync::OwnedMutexGuard::mutex(&self.compiler),
            &db.topology_validation_lock,
        )
    }

    /// Candidate version, still uncommitted and inactive.
    #[must_use]
    pub fn target_version(&self) -> TopologyVersion {
        self.input.descriptor().target_version
    }
    /// Requirements consumed by this image; they grant no target output permit.
    #[must_use]
    pub fn root(&self) -> &TopologyMigrationRoot {
        self.input.root()
    }
    /// Number of verified local frames successfully restored through existing operator codecs.
    #[must_use]
    pub const fn restored_frame_count(&self) -> usize {
        self.restored_frames
    }
    /// Accounted managed state in the prepared graph, bounded by the configured state budget.
    #[must_use]
    pub fn managed_state_bytes(&self) -> usize {
        self.graph.managed_state_accounted_bytes()
    }
    /// Preserved/new cursor origins. Sources have not started or acknowledged input.
    #[must_use]
    pub const fn source_positions(&self) -> &BTreeMap<String, PreparedTopologySourcePosition> {
        &self.sources
    }
    /// Original historical checkpoint metadata and identity. Its state buffers are released after
    /// decoding; no historical checkpoint is relabelled as target processing history.
    #[must_use]
    pub const fn parent_checkpoint(&self) -> &laminar_core::checkpoint::CommittedCheckpointIndex {
        &self.recovered.committed
    }
}

impl LaminarDB {
    /// Restore one private target from the exact held and authority-bound old cut.
    /// Replays the immutable candidate, checks its complete certified descriptor, loads verified
    /// local state through the strict parent recovery path, and preserves subscription incarnations
    /// and exclusive sequences. Sealed source cursors are validated without resolving them again.
    /// This neither mutates the live catalog/coordinator nor installs actors or opens intake.
    ///
    /// One image owns the existing compiler slot for its lifetime. The 45 second request budget,
    /// aggregate 16 MiB manifest limit, configured node-read and managed-state budgets bound restore.
    /// Cancellation/error drops the partial image and releases the slot, retaining the parent hold.
    ///
    /// # Errors
    /// Rejects absent holds, stale full-roster/process/assignment authority, divergent compilation,
    /// damaged state/output artifacts, missing state or subscription frontiers and exceeded bounds.
    pub async fn prepare_cluster_topology_restore(
        &self,
        operation_id: TopologyOperationId,
    ) -> Result<PreparedTopologyRestore, DbError> {
        self.ensure_topology_root_available()?;
        let compiler = Arc::clone(&self.topology_validation_lock)
            .try_lock_owned()
            .map_err(|_| TopologyError::PlanningBusy)?;
        // Keep the permit outside the timed future so cancellation drops partial state first.
        let (candidate, graph, input, recovered, sources, restored_frames) =
            tokio::time::timeout(std::time::Duration::from_secs(45), async {
                let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
                    TopologyError::Protocol("restore requires the configured controller".into())
                })?;
                let input = controller.topology_restore_input(operation_id).await?;
                self.validate_bound_parent_pipeline(&input.descriptor().parent_pipeline)
                    .await?;
                let parent_count = input.plan().parent_manifest.entry_count as usize;
                let parent_entries = input.target().entries.get(..parent_count).ok_or_else(|| {
                    TopologyError::Invalid("restore target is shorter than its parent".into())
                })?;
                if self.catalog_manifest_inventory()? != parent_entries {
                    return Err(TopologyError::Conflict(
                        "restore requires the exact live parent inventory".into(),
                    ).into());
                }
                if self.topology_definition_identities()?.pipeline != input.descriptor().parent_pipeline {
                    return Err(TopologyError::Conflict(
                        "live parent differs from the certified restore parent".into(),
                    ).into());
                }
                let candidate = self.isolated_topology_catalog()?;
                for entry in &input.target().entries {
                    super::planning::replay_entry(&candidate, entry).await?;
                }
                if candidate.reconcile_catalog_manifest_inventory(input.target())? != input.target().entries {
                    return Err(TopologyError::Invalid(
                        "restore candidate changed its exact incarnations".into(),
                    ).into());
                }
                let identities = candidate.topology_definition_identities()?;
                if identities.pipeline != input.descriptor().target_pipeline
                    || identities.environment_sha256 != input.descriptor().environment_sha256
                {
                    return Err(TopologyError::Conflict(
                        "restore candidate differs from the certified target environment".into(),
                    ).into());
                }
                bind_preserved_subscriptions(&candidate, &input)?;
                let scope = crate::operator::sql_query::ClusterShuffleConfig {
                    registry: self.vnode_registry.lock().clone().ok_or(TopologyError::Fenced)?,
                    sender: self.shuffle_sender.lock().clone().ok_or(TopologyError::Fenced)?,
                    receiver: self.shuffle_receiver.lock().clone().ok_or(TopologyError::Fenced)?,
                    self_id: laminar_core::state::NodeId(input.process().participant.node_id),
                };
                let assignment = scope.registry.versioned_snapshot();
                let owner_ids = assignment.owners().iter().map(|owner| owner.0).collect::<Vec<_>>();
                if assignment.version() != input.plan().assignment.assignment_version
                    || !input.plan().assignment.matches_owner_map(&owner_ids)
                {
                    return Err(TopologyError::Fenced.into());
                }
                let (description, graph) = candidate.compile_topology_restore_graph(&input, scope).await?;
                let objects = super::planning::describe_catalog(
                    &candidate, input.target(), &identities, &description, parent_count,
                )?;
                if objects.into_values().collect::<Vec<_>>() != input.descriptor().objects {
                    return Err(TopologyError::Conflict(
                        "restore graph differs from its certified descriptor".into(),
                    ).into());
                }
                drop(description);
                let budget = self.config.pipeline_max_managed_state_bytes.ok_or_else(|| {
                    TopologyError::Invalid("restore has no managed-state budget".into())
                })?;
                let objects = self.checkpoint_object_store()?.ok_or_else(|| {
                    TopologyError::Protocol("restore requires configured checkpoint storage".into())
                })?;
                let store = ObjectStoreCheckpointStore::new(objects, "")
                    .with_participant_id(input.process().participant.node_id)
                    .with_key_group_count(self.checkpoint_key_groups())
                    .with_max_node_data_bytes(
                        self.config.checkpoint.as_ref().and_then(|c| c.max_node_data_bytes)
                            .unwrap_or(laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES),
                    )?;
                let mut recovered = crate::recovery_manager::RecoveryManager::new(
                    &store, &input.descriptor().parent_pipeline,
                    &input.descriptor().deployment_id, CheckpointScope::Cluster,
                ).recover_topology_root(&input, budget).await?;
                let (graph, restored_frames) = restore_local_frames(graph, &recovered, &input)?;
                validate_subscription_frontiers(&graph, &input)?;
                // Release verified encoded buffers immediately after decoding, before broker I/O.
                recovered.state_frames.clear();
                let sources = prepare_source_positions(&candidate, &input).await?;
                if !controller.topology_restore_input(operation_id).await?.same_restore_requirements(&input) {
                    return Err(TopologyError::Fenced.into());
                }
                self.ensure_topology_root_available()?;
                self.validate_bound_parent_pipeline(&input.descriptor().parent_pipeline).await?;
                if self.topology_definition_identities()?.pipeline != input.descriptor().parent_pipeline {
                    return Err(TopologyError::Fenced.into());
                }
                Ok::<_, DbError>((candidate, graph, input, recovered, sources, restored_frames))
            })
            .await
            .map_err(|_| TopologyError::Contended)??;
        Ok(PreparedTopologyRestore {
            candidate,
            graph,
            input,
            recovered,
            sources,
            restored_frames,
            parent_retirement_observed: false,
            compiler,
        })
    }
}

async fn prepare_source_positions(
    candidate: &LaminarDB,
    input: &TopologyRestoreInput,
) -> Result<BTreeMap<String, PreparedTopologySourcePosition>, DbError> {
    let registrations = candidate.connector_manager.lock().sources().clone();
    let mut sources = BTreeMap::new();
    for name in candidate.catalog.list_sources() {
        let registration = registrations
            .get(&name)
            .ok_or_else(|| TopologyError::Invalid("restore source has no connector".into()))?;
        let config = candidate.build_registered_source_config(&name, registration)?;
        let mut connector = candidate.connector_registry.create_source(&config, None)?;
        let position = if let Some(checkpoint) = input.checkpoint().source_offsets.get(&name) {
            let scoped = connector.contract(&config)?.topology
                == laminar_connectors::connector::SourceTopology::Splittable;
            crate::pipeline_lifecycle::validate_source_recovery_assignment(
                &name,
                scoped,
                Some(checkpoint),
                std::num::NonZeroU64::new(input.plan().assignment.assignment_version),
            )?;
            PreparedTopologySourcePosition::Preserved {
                attempt: CheckpointAttempt::canonical(input.checkpoint().epoch),
                checkpoint: crate::checkpoint_coordinator::connector_to_source_checkpoint(
                    checkpoint,
                ),
            }
        } else {
            let initialization = input
                .root()
                .source_initializations
                .iter()
                .find(|source| source.name == name)
                .ok_or_else(|| {
                    TopologyError::Invalid("new source has no sealed initialization".into())
                })?;
            let checkpoint = crate::checkpoint_coordinator::connector_to_source_checkpoint(
                &initialization.checkpoint,
            );
            connector
                .validate_initial_position(&config, &checkpoint)
                .await
                .map_err(|error| {
                    TopologyError::Unsupported(format!(
                        "source '{name}' sealed cursor validation: {error}"
                    ))
                })?;
            PreparedTopologySourcePosition::Initialized { checkpoint }
        };
        sources.insert(name, position);
    }
    Ok(sources)
}

fn bind_preserved_subscriptions(
    candidate: &LaminarDB,
    input: &TopologyRestoreInput,
) -> Result<(), DbError> {
    let mut streams = candidate.connector_manager.lock().streams().clone();
    for subscription in &input.root().subscriptions {
        let certificate = &subscription.target_certificate;
        let stream = streams.get_mut(&certificate.stream_id).ok_or_else(|| {
            TopologyError::Invalid("preserved subscription has no target stream".into())
        })?;
        if stream.subscription_output.is_none()
            || stream.catalog_generation != certificate.catalog_generation
        {
            return Err(TopologyError::Invalid(
                "preserved subscription has a different target incarnation".into(),
            )
            .into());
        }
        stream.subscription_certificate = Some(certificate.clone());
    }
    if streams.values().any(|stream| {
        stream.subscription_output.is_some() && stream.subscription_certificate.is_none()
    }) {
        return Err(TopologyError::Unsupported(
            "new subscription output has no initialization contract".into(),
        )
        .into());
    }
    candidate
        .connector_manager
        .lock()
        .install_stream_subscription_certificates(&streams)
}

fn restore_local_frames(
    graph: crate::operator_graph::OperatorGraph,
    recovered: &crate::recovery_manager::RecoveredState,
    input: &TopologyRestoreInput,
) -> Result<(crate::operator_graph::OperatorGraph, usize), DbError> {
    let mut whole = Vec::new();
    let mut vnodes = Vec::new();
    if recovered.reassigned {
        return Err(TopologyError::Fenced.into());
    }
    for frame in &recovered.state_frames {
        if frame.participant_id != input.process().participant.node_id {
            return Err(TopologyError::Fenced.into());
        }
        match &frame.key {
            StateFrameKey::OperatorWhole { operator_id } => {
                whole.push((graph_name(operator_id)?, frame.payload.clone()));
            }
            StateFrameKey::Vnode { operator_id, vnode } => vnodes.push((
                graph_name(operator_id)?,
                u32::from(*vnode),
                frame.payload.clone(),
            )),
        }
    }
    graph.restore_topology_state_frames(&whole, &vnodes, input)
}

fn graph_name(operator_id: &str) -> Result<String, DbError> {
    operator_id
        .strip_prefix("graph:")
        .map(str::to_owned)
        .ok_or_else(|| {
            TopologyError::Unsupported("migration restore has no non-graph state mapping".into())
                .into()
        })
}

fn validate_subscription_frontiers(
    graph: &crate::operator_graph::OperatorGraph,
    input: &TopologyRestoreInput,
) -> Result<(), DbError> {
    let captures = graph.capture_subscription_frontiers()?;
    if captures.len() != input.root().subscriptions.len() {
        return Err(TopologyError::Invalid(
            "restored subscription roster differs from the root".into(),
        )
        .into());
    }
    for (capture, mapping) in captures.iter().zip(&input.root().subscriptions) {
        let expected = mapping
            .frontiers
            .iter()
            .filter(|frontier| {
                input
                    .owned_vnodes()
                    .binary_search(&u32::from(frontier.partition.get()))
                    .is_ok()
            })
            .collect::<Vec<_>>();
        if capture.certificate.as_ref() != &mapping.target_certificate
            || capture.frontiers.iter().collect::<Vec<_>>() != expected
        {
            return Err(TopologyError::Invalid(
                "restored subscription identity or exclusive sequence differs from the root".into(),
            )
            .into());
        }
    }
    Ok(())
}
