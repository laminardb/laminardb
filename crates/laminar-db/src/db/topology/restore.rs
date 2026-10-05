//! Private target images reuse the isolated compiler and exact-cut recovery loader.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use laminar_core::checkpoint::{CheckpointAttempt, CheckpointScope, ObjectStoreCheckpointStore};
use laminar_core::cluster::control::{
    RecoveryAnnouncement, TopologyError, TopologyMigrationRoot, TopologyOperationId,
    TopologyRecoveryCut, TopologyRecoveryInput, TopologyRestoreInput, TopologyVersion,
};

use super::{DbError, LaminarDB};

pub(super) enum TopologyRestorePurpose {
    CutPreparation,
    MigrationInstallation,
    Recovery,
    CoordinatedRecovery(Box<RecoveryAnnouncement>),
}

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

impl PreparedTopologySourcePosition {
    /// Build the atomic startup position without inventing checkpoint history for a new source.
    /// The installer must still revalidate Commit, current ownership and sealed availability,
    /// keep source intake held, and wait for participant-complete target Release.
    #[must_use]
    pub fn startup_position(&self) -> laminar_connectors::connector::SourcePosition {
        match self {
            Self::Preserved {
                attempt,
                checkpoint,
            } => laminar_connectors::connector::SourcePosition::Resume {
                attempt: *attempt,
                checkpoint: checkpoint.clone(),
            },
            Self::Initialized { checkpoint } => {
                laminar_connectors::connector::SourcePosition::Initialized {
                    checkpoint: checkpoint.clone(),
                }
            }
        }
    }
}

/// One unstarted target state image. It owns the existing local compiler slot until dropped.
/// It has no source/sink actors or installation/output authority. Existing fenced parent transport
/// handles provide channel decoding context; no receiver or sender is started by preparation.
/// Even a successful preparation requires observed predecessor retirement, fresh authority checks, Commit and
/// coordinated target Release before it can become a runtime graph.
pub struct PreparedTopologyRestore {
    pub(crate) candidate: LaminarDB,
    pub(crate) graph: crate::operator_graph::OperatorGraph,
    pub(crate) input: TopologyRestoreInput,
    // A selected recovery image cannot use the original migration installation/Release path.
    recovery: Option<Box<TopologyRecoveryInput>>,
    pub(super) recovery_start: Option<RecoveryAnnouncement>,
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
                "recovery_cut",
                &self.recovery.as_ref().map(|input| input.cut()),
            )
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
    pub(crate) fn into_runtime(
        self,
    ) -> Result<
        (
            crate::operator_graph::OperatorGraph,
            TopologyRuntimeMetadata,
        ),
        DbError,
    > {
        let Self {
            candidate,
            graph,
            input,
            recovery,
            recovery_start,
            recovered,
            sources,
            compiler,
            ..
        } = self;
        let recovery = match (recovery, recovery_start) {
            (Some(selection), Some(start)) => Some(TopologyRecoveryRuntime {
                selection,
                start,
                released: false,
            }),
            (None, None) => None,
            _ => return Err(TopologyError::Fenced.into()),
        };
        drop(candidate);
        Ok((
            graph,
            TopologyRuntimeMetadata {
                input,
                recovery,
                recovered,
                sources,
                _compiler: compiler,
            },
        ))
    }

    /// Whether this image was authorized from an irreversible target Commit. It remains private;
    /// receivers, state installation, sinks and participant-complete Release are still required.
    #[must_use]
    pub const fn is_committed(&self) -> bool {
        self.input.is_committed()
    }
    /// Whether this image's parent actors were observed terminal after exact authority checks.
    /// This is a local observation, not a durable readiness receipt or target output permit.
    /// A future installer must revalidate current authority and the retired runtime boundary.
    #[must_use]
    pub const fn parent_retirement_observed(&self) -> bool {
        self.parent_retirement_observed
    }

    pub(super) fn belongs_to(&self, db: &LaminarDB) -> bool {
        self.recovery.is_none()
            && self.recovery_start.is_none()
            && Arc::ptr_eq(
                tokio::sync::OwnedMutexGuard::mutex(&self.compiler),
                &db.topology_validation_lock,
            )
    }

    pub(crate) fn belongs_to_recovery(&self, db: &LaminarDB, start: &RecoveryAnnouncement) -> bool {
        self.recovery_start.as_ref() == Some(start)
            && self.recovery.is_some()
            && Arc::ptr_eq(
                tokio::sync::OwnedMutexGuard::mutex(&self.compiler),
                &db.topology_validation_lock,
            )
    }

    /// Target version of this private image; Commit does not activate it.
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
        self.input.checkpoint()
    }

    /// Selected private recovery authority, absent for original migration installation images.
    /// This never authorizes reuse of the original runtime's installation receipt or Release.
    #[must_use]
    pub fn recovery_input(&self) -> Option<&TopologyRecoveryInput> {
        self.recovery.as_deref()
    }

    /// Exact cut supplying this image, with its original parent or target pipeline identity.
    #[must_use]
    pub const fn recovery_checkpoint(&self) -> &laminar_core::checkpoint::CommittedCheckpointIndex {
        &self.recovered.committed
    }
}

/// Control-path ownership of the exact root until the restored graph reaches runtime readiness.
#[derive(Clone)]
pub(crate) struct TopologyRecoveryRuntime {
    pub(crate) selection: Box<TopologyRecoveryInput>,
    pub(crate) start: RecoveryAnnouncement,
    // Set only while consuming the exact durable coordinated Release against these live actors.
    pub(crate) released: bool,
}

pub(crate) struct TopologyRuntimeMetadata {
    pub(crate) input: TopologyRestoreInput,
    pub(crate) recovery: Option<TopologyRecoveryRuntime>,
    pub(crate) recovered: crate::recovery_manager::RecoveredState,
    pub(crate) sources: BTreeMap<String, PreparedTopologySourcePosition>,
    _compiler: tokio::sync::OwnedMutexGuard<()>,
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
        self.prepare_topology_restore_image(operation_id, TopologyRestorePurpose::CutPreparation)
            .await
    }

    /// Reconstruct the current committed target from its exact migration root before its first
    /// checkpoint. Reuses the same strict historical-parent loader and actual target codecs.
    /// A Created DB or still-held retired parent can reconstruct; the private image never starts
    /// actors, changes the local catalog/coordinator, acknowledges input or opens intake.
    ///
    /// # Errors
    /// Rejects uncommitted/obsolete targets, live or faulted local actors, changed owner maps,
    /// stale current process/adoption, missing/corrupt artifacts or the existing 45 second budget.
    pub async fn recover_committed_cluster_topology(
        &self,
        operation_id: TopologyOperationId,
    ) -> Result<PreparedTopologyRestore, DbError> {
        self.prepare_topology_restore_image(
            operation_id,
            TopologyRestorePurpose::MigrationInstallation,
        )
        .await
    }

    pub(super) async fn prepare_topology_restore_image(
        &self,
        operation_id: TopologyOperationId,
        purpose: TopologyRestorePurpose,
    ) -> Result<PreparedTopologyRestore, DbError> {
        let committed = !matches!(purpose, TopologyRestorePurpose::CutPreparation);
        let recovery_start = match &purpose {
            TopologyRestorePurpose::CoordinatedRecovery(start) => Some((**start).clone()),
            _ => None,
        };
        self.ensure_topology_restore_purpose(&purpose)?;
        let compiler = Arc::clone(&self.topology_validation_lock)
            .try_lock_owned()
            .map_err(|_| TopologyError::PlanningBusy)?;
        // Keep the permit outside the timed future so cancellation drops partial state first.
        let (candidate, graph, input, recovery, recovered, sources, restored_frames) =
            tokio::time::timeout(std::time::Duration::from_secs(45), async {
                let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
                    TopologyError::Protocol("restore requires the configured controller".into())
                })?;
                let recovery = if committed {
                    let selection = if let Some(start) = &recovery_start {
                        controller.topology_recovery_input(&start.round, super::recovery_runtime::start_epoch(start)?).await?
                    } else {
                        controller.committed_topology_recovery_input(operation_id).await?
                    };
                    if selection.migration().operation().operation_id != operation_id {
                        return Err(TopologyError::Fenced.into());
                    }
                    if matches!(purpose, TopologyRestorePurpose::MigrationInstallation)
                        && selection.cut() != TopologyRecoveryCut::MigrationRoot {
                        return Err(TopologyError::Conflict(
                            "target checkpoint has committed; reconstruction requires coordinated target recovery".into(),
                        ).into());
                    }
                    if super::DbState::load(&self.state) == super::DbState::ShuttingDown {
                        self.stop_pipeline_for_topology_retirement().await?;
                    }
                    Some(Box::new(selection))
                } else {
                    None
                };
                let input = if let Some(selection) = &recovery {
                    selection.migration().clone()
                } else {
                    controller.topology_restore_input(operation_id).await?
                };
                if !committed {
                    self.validate_bound_parent_pipeline(&input.descriptor().parent_pipeline).await?;
                }
                let local = self.catalog_manifest_inventory()?;
                let catalog_matches = local == input.parent().entries
                    || (committed && (local.is_empty() || local == input.target().entries));
                if !catalog_matches {
                    return Err(TopologyError::Conflict(
                        "restore requires the exact live parent inventory".into(),
                    ).into());
                }
                if !committed && self.topology_definition_identities()?.pipeline != input.descriptor().parent_pipeline {
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
                let mut scope = crate::operator::sql_query::ClusterShuffleConfig {
                    registry: self.vnode_registry.lock().clone().ok_or(TopologyError::Fenced)?,
                    sender: self.shuffle_sender.lock().clone().ok_or(TopologyError::Fenced)?,
                    receiver: self.shuffle_receiver.lock().clone().ok_or(TopologyError::Fenced)?,
                    topology: None,
                    self_id: laminar_core::state::NodeId(input.process().participant.node_id),
                };
                scope.topology = scope.sender.topology_fence();
                scope.ensure_topology_current()?;
                let parent_topology = if input.plan().expected_parent == TopologyVersion::LEGACY_BASELINE {
                    None
                } else {
                    Some(laminar_core::shuffle::ShuffleTopologyFence::from_manifest(
                        input.plan().expected_parent, &input.plan().parent_manifest,
                    ).map_err(|error| TopologyError::Invalid(error.to_string()))?)
                };
                let target_topology = laminar_core::shuffle::ShuffleTopologyFence::from_manifest(
                    input.descriptor().target_version, &input.plan().target_manifest,
                ).map_err(|error| TopologyError::Invalid(error.to_string()))?;
                if scope.receiver.topology_fence() != scope.topology
                    || (scope.topology != parent_topology
                        && !(committed && (scope.topology == Some(target_topology)
                            || (super::DbState::load(&self.state) == super::DbState::Created && scope.topology.is_none())))) {
                    return Err(TopologyError::Fenced.into());
                }
                let assignment = scope.registry.versioned_snapshot();
                let owner_ids = assignment.owners().iter().map(|owner| owner.0).collect::<Vec<_>>();
                if assignment.version() != input.assignment().assignment_version
                    || !input.assignment().matches_owner_map(&owner_ids)
                {
                    return Err(TopologyError::Fenced.into());
                }
                let (description, graph) = candidate.compile_topology_restore_graph(&input, scope).await?;
                let objects = super::planning::describe_catalog(
                    &candidate, input.target(), &identities, &description, input.parent(),
                )?;
                if objects.into_values().ne(input.descriptor().objects.iter().filter(|object| object.transition != super::ClusterTopologyObjectTransition::Remove).cloned()) {
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
                let selected_pipeline = recovery.as_ref().map_or(
                    &input.descriptor().parent_pipeline, |selection| &selection.checkpoint().pipeline_identity,
                );
                let reader = crate::recovery_manager::RecoveryManager::new(
                    &store, selected_pipeline,
                    &input.descriptor().deployment_id, CheckpointScope::Cluster,
                );
                let mut recovered = if let Some(selection) = &recovery {
                    reader.recover_topology_selection(selection, budget).await?
                } else {
                    reader.recover_topology_root(&input, budget).await?
                };
                let (graph, restored_frames) = graph.restore_topology_state_frames(&recovered, &input)?;
                if recovery.as_ref().is_some_and(|selection| selection.cut() == TopologyRecoveryCut::TargetCheckpoint) {
                    super::recovery::validate_target_subscription_frontiers(&graph, &recovered, &input)?;
                } else {
                    validate_subscription_frontiers(&graph, &input)?;
                }
                // Release verified encoded buffers immediately after decoding, before broker I/O.
                recovered.state_frames.clear();
                let sources = prepare_source_positions_at_cut(&candidate, &input, recovery.as_deref()).await?;
                let after = if let Some(selection) = &recovery {
                    let fresh = if let Some(start) = &recovery_start {
                        controller.topology_recovery_input(&start.round, super::recovery_runtime::start_epoch(start)?).await?
                    } else {
                        controller.committed_topology_recovery_input(operation_id).await?
                    };
                    if !fresh.same_restore_requirements(selection) {
                        return Err(TopologyError::Fenced.into());
                    }
                    fresh.migration().clone()
                } else {
                    controller.topology_restore_input(operation_id).await?
                };
                if !after.same_restore_requirements(&input) {
                    return Err(TopologyError::Fenced.into());
                }
                self.ensure_topology_restore_purpose(&purpose)?;
                if self.catalog_manifest_inventory()? != local { return Err(TopologyError::Fenced.into()); }
                if !committed {
                    self.validate_bound_parent_pipeline(&input.descriptor().parent_pipeline).await?;
                    if self.topology_definition_identities()?.pipeline != input.descriptor().parent_pipeline {
                        return Err(TopologyError::Fenced.into());
                    }
                }
                let recovery = matches!(purpose, TopologyRestorePurpose::Recovery | TopologyRestorePurpose::CoordinatedRecovery(_)).then_some(recovery).flatten();
                Ok::<_, DbError>((candidate, graph, input, recovery, recovered, sources, restored_frames))
            })
            .await
            .map_err(|_| TopologyError::Contended)??;
        Ok(PreparedTopologyRestore {
            candidate,
            graph,
            input,
            recovery,
            recovery_start,
            recovered,
            sources,
            restored_frames,
            parent_retirement_observed: false,
            compiler,
        })
    }

    fn ensure_topology_restore_purpose(
        &self,
        purpose: &TopologyRestorePurpose,
    ) -> Result<(), DbError> {
        if let TopologyRestorePurpose::CoordinatedRecovery(start) = purpose {
            self.ensure_topology_recovery_stopped(start)
        } else {
            self.ensure_topology_restore_available(!matches!(
                purpose,
                TopologyRestorePurpose::CutPreparation
            ))
        }
    }

    pub(super) fn ensure_topology_restore_available(&self, committed: bool) -> Result<(), DbError> {
        if !committed {
            if super::DbState::load(&self.state) == super::DbState::ShuttingDown {
                // A monitor restart may drop its private image after observed parent retirement.
                // The exact held parent/root still permit private reconstruction, never intake.
                return self.ensure_topology_retirement_available();
            }
            return self.ensure_topology_root_available();
        }
        if self.is_closed() {
            return Err(DbError::Shutdown);
        }
        let state = super::DbState::load(&self.state);
        if !self.is_cluster_runtime()
            || !matches!(
                state,
                super::DbState::Created | super::DbState::ShuttingDown
            )
            || (state == super::DbState::ShuttingDown
                && (!self.topology_cut_hold.load(Ordering::Acquire)
                    || !self.source_gate.load(Ordering::Acquire)
                    || !self.runtime_shutdown.read().is_cancelled()))
            || self.cluster_authority_revoked.load(Ordering::Acquire)
            || self.durable_terminal_recovery_fence.load(Ordering::Acquire)
            || self.terminal_pipeline_halt.load(Ordering::Acquire)
            || self.coordinated_recovery_in_progress()
            || self.pending_recovery_fault.load(Ordering::Acquire) != 0
            || self.last_fault.lock().is_some()
        {
            return Err(TopologyError::Conflict("committed reconstruction requires a Created DB or its still-held retired parent without local faults".into()).into());
        }
        self.ensure_catalog_cleanup_unfenced("committed topology reconstruction")
    }
}

pub(super) async fn prepare_source_positions(
    candidate: &LaminarDB,
    input: &TopologyRestoreInput,
) -> Result<BTreeMap<String, PreparedTopologySourcePosition>, DbError> {
    prepare_source_positions_at_cut(candidate, input, None).await
}

pub(super) async fn prepare_source_positions_at_cut(
    candidate: &LaminarDB,
    input: &TopologyRestoreInput,
    recovery: Option<&TopologyRecoveryInput>,
) -> Result<BTreeMap<String, PreparedTopologySourcePosition>, DbError> {
    let cut = recovery.map_or_else(|| input.checkpoint(), TopologyRecoveryInput::checkpoint);
    let registrations = candidate.connector_manager.lock().sources().clone();
    let mut sources = BTreeMap::new();
    for name in candidate.catalog.list_sources() {
        let registration = registrations
            .get(&name)
            .ok_or_else(|| TopologyError::Invalid("restore source has no connector".into()))?;
        let config = candidate.build_registered_source_config(&name, registration)?;
        let mut connector = candidate.connector_registry.create_source(&config, None)?;
        let target_cut = cut.pipeline_identity == input.descriptor().target_pipeline;
        let preserved = input.root().preserved_objects.iter().any(|object| {
            object.kind == laminar_core::cluster::control::CatalogObjectKind::Source
                && object.name == name
        });
        let position = if let Some(checkpoint) = cut
            .source_offsets
            .get(&name)
            .filter(|_| target_cut || preserved)
        {
            let scoped = connector.contract(&config)?.topology
                == laminar_connectors::connector::SourceTopology::Splittable;
            crate::pipeline_lifecycle::validate_source_recovery_assignment(
                &name,
                scoped,
                Some(checkpoint),
                cut.assignment_fence.as_ref().and_then(|assignment| {
                    std::num::NonZeroU64::new(assignment.assignment_version)
                }),
            )?;
            PreparedTopologySourcePosition::Preserved {
                attempt: CheckpointAttempt::canonical(cut.epoch),
                checkpoint: crate::checkpoint_coordinator::connector_to_source_checkpoint(
                    checkpoint,
                ),
            }
        } else {
            if recovery
                .is_some_and(|selection| selection.cut() == TopologyRecoveryCut::TargetCheckpoint)
            {
                return Err(TopologyError::Invalid(format!(
                    "target checkpoint has no committed progress for source '{name}'; root initialization cannot replace it",
                )).into());
            }
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

impl LaminarDB {
    pub(crate) fn bind_topology_subscriptions(
        &self,
        streams: &mut std::collections::HashMap<
            String,
            crate::connector_manager::StreamRegistration,
        >,
        input: &TopologyRestoreInput,
        schemas: &std::collections::HashMap<String, arrow_schema::SchemaRef>,
    ) -> Result<(), DbError> {
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
        for stream in streams
            .values_mut()
            .filter(|stream| stream.subscription_certificate.is_none())
        {
            let Some(output) = &stream.subscription_output else {
                continue;
            };
            let object = input
                .descriptor()
                .objects
                .iter()
                .find(|object| {
                    object.name == stream.name
                        && object.transition != super::ClusterTopologyObjectTransition::Remove
                })
                .ok_or_else(|| {
                    TopologyError::Invalid("new subscription has no descriptor".into())
                })?;
            if object.initialization
                != super::planning::TopologyInitialization::EmptyManagedStateAtCut
                || object.catalog_generation != stream.catalog_generation
                || input
                    .root()
                    .future_only_objects
                    .binary_search(&stream.name)
                    .is_err()
            {
                return Err(TopologyError::Invalid(
                    "new subscription has no empty-state cut contract".into(),
                )
                .into());
            }
            let schema = schemas.get(&stream.name).ok_or_else(|| {
                TopologyError::Invalid("new subscription has no resolved schema".into())
            })?;
            let schema_fingerprint =
                crate::pipeline_identity::subscription_schema_fingerprint(schema)?;
            if object.schema_sha256.as_deref() != Some(schema_fingerprint.to_hex().as_str()) {
                return Err(TopologyError::Invalid(
                    "new subscription schema differs from its descriptor".into(),
                )
                .into());
            }
            stream.subscription_certificate = Some(
                output.bind(
                    uuid::Uuid::parse_str(&input.descriptor().deployment_id)
                        .map_err(|error| TopologyError::Invalid(error.to_string()))?,
                    stream.catalog_generation,
                    &stream.name,
                    schema_fingerprint,
                    stream.subscription_retention_bytes,
                    input.descriptor().target_pipeline.clone(),
                    self.checkpoint_key_groups(),
                )?,
            );
        }
        self.connector_manager
            .lock()
            .install_stream_subscription_certificates(streams)
    }
}

fn validate_subscription_frontiers(
    graph: &crate::operator_graph::OperatorGraph,
    input: &TopologyRestoreInput,
) -> Result<(), DbError> {
    let captures = graph.capture_subscription_frontiers()?;
    let mut preserved = 0;
    for capture in &captures {
        let Some(mapping) =
            input.root().subscriptions.iter().find(|mapping| {
                mapping.target_certificate.stream_id == capture.certificate.stream_id
            })
        else {
            let initialized = input.descriptor().objects.iter().any(|object| {
                object.name == capture.certificate.stream_id
                    && object.initialization
                        == super::planning::TopologyInitialization::EmptyManagedStateAtCut
                    && object.catalog_generation == capture.certificate.catalog_generation
                    && input
                        .root()
                        .future_only_objects
                        .binary_search(&object.name)
                        .is_ok()
            });
            if !initialized
                || capture.certificate.pipeline_identity != input.descriptor().target_pipeline
                || capture.frontiers.iter().any(|frontier| {
                    frontier.through_sequence != laminar_core::checkpoint::PartitionSequence::FIRST
                })
            {
                return Err(TopologyError::Invalid(
                    "new subscription did not initialize at its first sequence".into(),
                )
                .into());
            }
            continue;
        };
        preserved += 1;
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
    if preserved != input.root().subscriptions.len() {
        return Err(TopologyError::Invalid(
            "restored subscription roster differs from the root".into(),
        )
        .into());
    }
    Ok(())
}
