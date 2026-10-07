//! Local topology validation against an authority-audited parent. No durable operation is admitted.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use laminar_core::checkpoint::PipelineIdentity;
use laminar_core::cluster::control::{
    CatalogManifest, CatalogManifestEntry, CatalogObjectKind, TopologyCatalogState, TopologyError,
    TopologyVersion,
};

use crate::db::{DbState, LaminarDB, RuntimeMode};
use crate::error::DbError;
use crate::operator::capability::{ManagedStateContract, OperatorStateClass};
use crate::pipeline_identity::{
    compatibility_digest, compatibility_identities, PipelineCompatibilityIdentities,
    PipelineIdentityContext, PipelineRegistrations,
};
use crate::pipeline_lifecycle::PlannedTopologyGraph;

const VALIDATION_FORMAT_VERSION: u16 = 3;
pub(super) const MAX_VALIDATION_OBJECTS: usize = 256;
const MAX_VALIDATION_STATEMENTS: usize = 64;
const MAX_VALIDATION_SQL_BYTES: usize = 256 * 1024;

pub use laminar_core::cluster::control::topology::{
    ClusterTopologyObjectPlan, ClusterTopologyObjectTransition, ClusterTopologyValidation,
    TopologyActivationRequirement, TopologyInitialization, TopologyValidationScope,
};

impl LaminarDB {
    /// Compile a topology candidate separately from the active graph.
    ///
    /// Each entry is one typed CREATE, CREATE OR REPLACE or DROP SOURCE/STREAM/SINK without CASCADE.
    /// Replacement preserves compatible state by default. Ordered DROP/CREATE explicitly resets
    /// the affected objects to future-only input under new incarnations; reset dependents first.
    /// Supports replayable sources, supported managed/stateless streams and durable sinks while
    /// proving unchanged definitions remain compatible. New managed state starts empty at the cut;
    /// input that can retract an unavailable prefix is rejected. No source is opened
    /// or polled, no sink is opened or published, and no authority/checkpoint ID is written.
    /// A successful response does not authorize SQL submission, restore or target activation.
    ///
    /// One compiler per process, 64 statements/256 KiB input, 256 parent/target object mappings, a 1 MiB
    /// descriptor and a 30 second end-to-end deadline bound control-path resource use.
    ///
    /// # Errors
    /// Rejects parent conflicts, unsupported mutations, uncertified execution or
    /// connector contracts, divergent local definitions, malformed input and exceeded bounds.
    pub async fn validate_cluster_topology_change(
        &self,
        expected_parent: TopologyVersion,
        statements: &[String],
    ) -> Result<ClusterTopologyValidation, DbError> {
        self.plan_cluster_topology_change(expected_parent, statements)
            .await
            .map(|(report, _)| report)
    }

    pub(in crate::db) async fn plan_cluster_topology_change(
        &self,
        expected_parent: TopologyVersion,
        statements: &[String],
    ) -> Result<(ClusterTopologyValidation, CatalogManifest), DbError> {
        validate_request_bounds(statements)?;
        if !self.is_cluster_runtime() {
            return Err(TopologyError::Unsupported(
                "topology validation requires cluster mode".into(),
            )
            .into());
        }
        if !self.connector_manager.lock().process_functions().is_empty() {
            return Err(TopologyError::Unsupported(
                "process bindings are immutable; catalog changes require a new checkpoint namespace"
                    .into(),
            )
            .into());
        }
        let _compiler = self
            .topology_validation_lock
            .try_lock()
            .map_err(|_| TopologyError::PlanningBusy)?;
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            Box::pin(self.validate_topology_candidate(expected_parent, statements)),
        )
        .await
        .map_err(|_| TopologyError::PlanningTimedOut)?
    }

    async fn validate_topology_candidate(
        &self,
        expected_parent: TopologyVersion,
        statements: &[String],
    ) -> Result<(ClusterTopologyValidation, CatalogManifest), DbError> {
        let active_inventory = {
            let _catalog_read = self.topology_ddl_lock.read().await;
            self.ensure_validation_catalog_available()?;
            self.catalog_manifest_inventory()?
        };
        let store = self.catalog_manifest_store.lock().clone().ok_or_else(|| {
            TopologyError::Protocol(
                "topology validation requires a configured catalog authority".into(),
            )
        })?;
        let (parent, state) = store.load_with_topology().await?.ok_or_else(|| {
            TopologyError::Conflict("no sealed parent catalog; initialize and explicitly adopt the legacy baseline first".into())
        })?;
        let TopologyCatalogState::Versioned {
            baseline,
            committed,
        } = &state
        else {
            return Err(TopologyError::Protocol(
                "legacy catalog must be explicitly adopted before topology validation".into(),
            )
            .into());
        };
        if state.committed_version() != Some(expected_parent) {
            return Err(TopologyError::Conflict(format!(
                "expected parent {expected_parent:?}, authority has {:?}",
                state.committed_version()
            ))
            .into());
        }
        if parent.entries.len() > MAX_VALIDATION_OBJECTS {
            return Err(TopologyError::Unsupported(format!(
                "candidate exceeds the {MAX_VALIDATION_OBJECTS} object planning bound"
            ))
            .into());
        }
        if active_inventory != parent.entries {
            return Err(TopologyError::Conflict("local catalog is not the exact committed parent inventory; complete catalog replay before validation".into()).into());
        }
        let active_identities = self.topology_definition_identities()?;
        self.validate_bound_parent_pipeline(&active_identities.pipeline)
            .await?;
        let candidate = self.isolated_topology_catalog()?;
        for entry in &parent.entries {
            replay_entry(&candidate, entry).await?;
        }
        if candidate.reconcile_catalog_manifest_inventory(&parent)? != parent.entries {
            return Err(TopologyError::Invalid(
                "isolated parent replay changed its exact inventory or incarnations".into(),
            )
            .into());
        }
        let parent_identities = candidate.topology_definition_identities()?;
        if parent_identities != active_identities {
            return Err(TopologyError::Conflict("resolved parent definitions differ from the live catalog; check non-secret environment/config references and connector contracts".into()).into());
        }
        let parent_graph = candidate.plan_topology_graph().await?;
        let parent_objects = describe_catalog(
            &candidate,
            &parent,
            &parent_identities,
            &parent_graph,
            &parent,
        )?;
        drop(parent_graph);
        let retired = store.retired_topology_generations().await?;
        let target = super::catalog_changes::apply_catalog_changes(
            &candidate, &parent, statements, &retired,
        )
        .await?;
        let target_identities = candidate.topology_definition_identities()?;
        let target_graph = candidate.plan_topology_graph().await?;
        super::catalog_changes::validate_prepared_targets(&candidate, &target)?;
        let objects = describe_catalog(
            &candidate,
            &target,
            &target_identities,
            &target_graph,
            &parent,
        )?;
        if parent_identities.environment_sha256 != target_identities.environment_sha256 {
            return Err(TopologyError::Unsupported(
                "candidate changes the global state/routing/delivery ABI".into(),
            )
            .into());
        }
        let objects = classify_object_transitions(&parent_objects, objects, statements)?;
        // No stale response can masquerade as validation against a newer parent. Normal lease
        // renewals/checkpoints do not invalidate a read-only plan; the exact catalog must match.
        if store.topology_state().await? != state
            || self.topology_definition_identities()? != active_identities
        {
            return Err(TopologyError::Conflict("parent authority or local definitions changed during validation; retry against the observed parent".into()).into());
        }
        self.ensure_validation_catalog_available()?;
        self.validate_bound_parent_pipeline(&active_identities.pipeline)
            .await?;
        let target_version = expected_parent.successor()?;
        let target_manifest = target.reference().map_err(TopologyError::from)?;
        let mut report = ClusterTopologyValidation {
            validation_format_version: VALIDATION_FORMAT_VERSION,
            scope: TopologyValidationScope::LocalCandidatePlan,
            deployment_id: baseline.deployment_id.clone(),
            parent_version: expected_parent,
            target_version,
            parent_manifest: committed.as_ref().map_or_else(
                || baseline.manifest.clone(),
                |commit| commit.manifest.clone(),
            ),
            target_manifest,
            parent_pipeline: parent_identities.pipeline,
            target_pipeline: target_identities.pipeline,
            environment_sha256: parent_identities.environment_sha256,
            compatibility_sha256: String::new(),
            statements: statements.to_vec(),
            objects,
            requires_processing_pause: true,
            required_before_activation: vec![
                TopologyActivationRequirement::ParticipantPlanAgreement,
                TopologyActivationRequirement::ReconciledCheckpointCut,
                TopologyActivationRequirement::DurableInitializationAndProgress,
                TopologyActivationRequirement::ObservedActorRetirement,
                TopologyActivationRequirement::AtomicTargetCommit,
                TopologyActivationRequirement::InstalledTargetRelease,
            ],
        };
        report.compatibility_sha256 = report.descriptor_digest()?;
        report.validate_catalogs(&parent, &target)?;
        Ok((report, target))
    }

    fn ensure_validation_catalog_available(&self) -> Result<(), DbError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(DbError::Shutdown);
        }
        if !matches!(
            DbState::load(&self.state),
            DbState::Created | DbState::Running
        ) {
            return Err(TopologyError::Conflict("catalog validation requires a Created or Running database; finish recovery/startup first".into()).into());
        }
        self.ensure_catalog_cleanup_unfenced("topology validation")?;
        if !self.custom_udfs.is_empty()
            || !self.custom_udafs.is_empty()
            || !self.physical_optimizer_rules.is_empty()
        {
            return Err(TopologyError::Unsupported(
                "custom function or optimizer implementation identity is not bound by the migration descriptor"
                    .into(),
            )
            .into());
        }
        Ok(())
    }

    pub(in crate::db) fn isolated_topology_catalog(&self) -> Result<LaminarDB, DbError> {
        let mut candidate = Self::open_with_config_and_vars_and_rules(
            self.config.clone(),
            self.config_vars.as_ref().clone(),
            &self.physical_optimizer_rules,
            self.pipeline_target_partitions,
            RuntimeMode::Cluster,
        )?;
        candidate.catalog = Arc::new(crate::catalog::SourceCatalog::for_topology_planning(
            &self.config,
        ));
        candidate.connector_registry = Arc::clone(&self.connector_registry);
        for registration in self.connector_manager.lock().process_functions().values() {
            candidate
                .connector_manager
                .lock()
                .register_process_function(registration.clone());
        }
        candidate.topology_planning_ownership_scope =
            Some(self.has_cluster_query_ownership_scope());
        *candidate.vnode_registry.lock() =
            Some(Arc::new(laminar_core::state::VnodeRegistry::single_owner(
                u32::from(self.checkpoint_key_groups()),
                laminar_core::state::LOCAL_NODE_ID,
            )));
        Ok(candidate)
    }

    pub(crate) fn topology_definition_identities(
        &self,
    ) -> Result<PipelineCompatibilityIdentities, DbError> {
        let vnode_count = self.checkpoint_key_groups().into();
        let manager = self.connector_manager.lock();
        compatibility_identities(&PipelineIdentityContext::new(
            &self.config,
            &self.catalog,
            &self.connector_registry,
            PipelineRegistrations::new(
                manager.sources().values(),
                manager.sinks().values(),
                manager.streams().values(),
                manager.tables().values(),
            ),
            vnode_count,
        ))
    }

    pub(super) async fn validate_bound_parent_pipeline(
        &self,
        expected: &PipelineIdentity,
    ) -> Result<(), DbError> {
        if DbState::load(&self.state) != DbState::Running {
            return Ok(());
        }
        let coordinator = self.coordinator.lock().await;
        let bound = coordinator
            .as_ref()
            .ok_or_else(|| {
                TopologyError::Conflict("running parent has no checkpoint coordinator".into())
            })?
            .bound_pipeline_identity()?;
        if &bound != expected {
            return Err(TopologyError::Conflict(
                "running checkpoint pipeline identity differs from the planned parent".into(),
            )
            .into());
        }
        Ok(())
    }
}

pub(super) fn validate_request_bounds(statements: &[String]) -> Result<(), DbError> {
    if statements.is_empty() || statements.len() > MAX_VALIDATION_STATEMENTS {
        return Err(TopologyError::Invalid(
            "validation requires 1..=64 individual topology DDL statements".into(),
        )
        .into());
    }
    let bytes = statements
        .iter()
        .try_fold(0_usize, |total, sql| total.checked_add(sql.len()));
    if bytes.is_none_or(|bytes| bytes > MAX_VALIDATION_SQL_BYTES) {
        return Err(TopologyError::Invalid("validation SQL exceeds 256 KiB".into()).into());
    }
    Ok(())
}

pub(super) async fn replay_entry(
    candidate: &LaminarDB,
    entry: &CatalogManifestEntry,
) -> Result<(), DbError> {
    let statement = super::catalog_changes::parse_one_change(&entry.ddl)?;
    let (name, kind, _) =
        super::super::validate_cluster_catalog_create(candidate, &entry.ddl, &statement)?;
    if name != entry.canonical_name || kind != entry.kind {
        return Err(
            TopologyError::Invalid("candidate DDL and catalog identity disagree".into()).into(),
        );
    }
    crate::ddl::schema_resolution::RESOLVED_SCHEMA
        .scope(
            entry.schema_binding.clone(),
            super::catalog_changes::apply_statement(candidate, &entry.ddl, &statement, &name),
        )
        .await
}

pub(super) fn describe_catalog(
    candidate: &LaminarDB,
    manifest: &CatalogManifest,
    identities: &PipelineCompatibilityIdentities,
    graph: &PlannedTopologyGraph,
    parent: &CatalogManifest,
) -> Result<BTreeMap<String, ClusterTopologyObjectPlan>, DbError> {
    let manager = candidate.connector_manager.lock();
    let mut objects = BTreeMap::<String, ClusterTopologyObjectPlan>::new();
    for entry in &manifest.entries {
        let name = &entry.canonical_name;
        let preserved = parent.entries.iter().any(|old| {
            &old.canonical_name == name
                && old.kind == entry.kind
                && old.catalog_generation == entry.catalog_generation
        });
        let dependencies = catalog_dependencies(&manager, entry)?;
        let dependency_evidence: Vec<_> = dependencies.iter().map(|dependency| {
            objects.get(dependency).map(|object| (&object.name, object.catalog_generation, &object.compatibility_sha256)).ok_or_else(|| TopologyError::Invalid(format!("'{name}' depends on '{dependency}' outside dependency-safe creation order")))
        }).collect::<Result<_, _>>()?;
        let definition_sha256 = identities
            .objects
            .get(name)
            .ok_or_else(|| {
                TopologyError::Invalid(format!(
                    "'{name}' is absent from canonical pipeline identity"
                ))
            })?
            .clone();
        let schema_sha256 = graph
            .schemas
            .get(name)
            .map(|schema| {
                crate::pipeline_identity::subscription_schema_fingerprint(schema)
                    .map(laminar_core::checkpoint::SubscriptionDigest::to_hex)
            })
            .transpose()?;
        let operator = graph.operators.get(name);
        if entry.kind == CatalogObjectKind::Stream && operator.is_none() {
            return Err(TopologyError::Invalid(format!(
                "stream '{name}' is absent from the physical candidate graph"
            ))
            .into());
        }
        let state_contract = operator
            .and_then(|capability| capability.managed_state)
            .map(state_contract_name);
        if !preserved {
            validate_future_state_inputs(name, operator, &dependencies, &objects, graph)?;
        }
        let identity = compatibility_digest(&(
            "laminardb-topology-object-mapping-v1",
            name,
            entry.kind,
            entry.catalog_generation,
            &identities.environment_sha256,
            &definition_sha256,
            &schema_sha256,
            operator,
            graph.connector_sha256.get(name),
            dependency_evidence,
        ))?;
        objects.insert(
            name.clone(),
            ClusterTopologyObjectPlan {
                name: name.clone(),
                kind: entry.kind,
                catalog_generation: entry.catalog_generation,
                transition: if preserved {
                    ClusterTopologyObjectTransition::Preserve
                } else {
                    ClusterTopologyObjectTransition::AddFutureOnly
                },
                initialization: if preserved {
                    TopologyInitialization::PreserveExactCut
                } else if entry.kind == CatalogObjectKind::Source {
                    TopologyInitialization::ResolveSourcePositionsOnce
                } else if state_contract.is_some() {
                    TopologyInitialization::EmptyManagedStateAtCut
                } else {
                    TopologyInitialization::FutureOnlyAtCut
                },
                definition_sha256,
                compatibility_sha256: identity,
                dependencies,
                schema_sha256,
                managed_state_contract: state_contract.map(str::to_owned),
            },
        );
    }
    Ok(objects)
}

fn catalog_dependencies(
    manager: &crate::connector_manager::ConnectorManager,
    entry: &CatalogManifestEntry,
) -> Result<Vec<String>, DbError> {
    let name = &entry.canonical_name;
    let mut dependencies = match entry.kind {
        CatalogObjectKind::Source => Vec::new(),
        CatalogObjectKind::Sink => vec![manager
            .sinks()
            .get(name)
            .ok_or_else(|| TopologyError::Invalid(format!("sink '{name}' has no registration")))?
            .input
            .clone()],
        CatalogObjectKind::Stream => {
            let stream = manager.streams().get(name).ok_or_else(|| {
                TopologyError::Invalid(format!("stream '{name}' has no registration"))
            })?;
            let mut references = crate::sql_analysis::extract_table_references(&stream.query_sql);
            for join in stream.join_config.iter().flatten() {
                match join {
                    laminar_sql::translator::JoinOperatorConfig::StreamStream(config) => {
                        references.insert(config.left_table.clone());
                        references.insert(config.right_table.clone());
                    }
                    laminar_sql::translator::JoinOperatorConfig::Temporal(config) => {
                        references.insert(config.left_table.clone());
                        references.insert(config.right_table.clone());
                    }
                    laminar_sql::translator::JoinOperatorConfig::Lookup(_) => {
                        return Err(TopologyError::Unsupported(format!(
                            "stream '{name}' has an unmapped join dependency contract"
                        ))
                        .into())
                    }
                }
            }
            if references.is_empty() {
                return Err(TopologyError::Unsupported(format!(
                    "stream '{name}' has no explicit input activation boundary"
                ))
                .into());
            }
            references.into_iter().collect()
        }
        _ => {
            return Err(TopologyError::Unsupported(format!(
                "'{name}' has an unsupported catalog kind for topology validation"
            ))
            .into())
        }
    };
    dependencies.sort_unstable();
    Ok(dependencies)
}

fn classify_object_transitions(
    parent: &BTreeMap<String, ClusterTopologyObjectPlan>,
    target: BTreeMap<String, ClusterTopologyObjectPlan>,
    statements: &[String],
) -> Result<Vec<ClusterTopologyObjectPlan>, DbError> {
    let replacements = statements
        .iter()
        .filter_map(|sql| match super::catalog_changes::parse_one_change(sql) {
            Ok(statement) if super::catalog_changes::is_replacement(&statement) => Some(
                super::super::catalog_create_identity(&statement).and_then(|identity| {
                    identity.map(|(name, _, _)| name).ok_or_else(|| {
                        TopologyError::Invalid("replacement has no typed identity".into()).into()
                    })
                }),
            ),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<std::collections::BTreeSet<_>, DbError>>()?;
    let mut retired = Vec::new();
    for (name, previous) in parent {
        match target.get(name) {
            Some(current) if current == previous => {}
            Some(current) if current.transition == ClusterTopologyObjectTransition::Preserve => {
                // A stateless query can change future computation with an unchanged schema and
                // dependency closure. Managed state and connectors require the exact definition.
                let compatible = replacements.contains(name)
                    && previous.kind == CatalogObjectKind::Stream
                    && previous.managed_state_contract.is_none()
                    && current.managed_state_contract.is_none()
                    && current.schema_sha256 == previous.schema_sha256
                    && current.dependencies == previous.dependencies
                    && current.dependencies.iter().all(|dependency| {
                        parent
                            .get(dependency)
                            .zip(target.get(dependency))
                            .is_some_and(|(old, new)| {
                                old.compatibility_sha256 == new.compatibility_sha256
                            })
                    });
                if !compatible {
                    return Err(TopologyError::Unsupported(format!(
                        "preserved object '{name}' changed its resolved state, schema, connector or dependency contract; explicitly DROP/CREATE it and its affected dependents for a future-only reset"
                    )).into());
                }
            }
            _ => {
                let mut removed = previous.clone();
                removed.transition = ClusterTopologyObjectTransition::Remove;
                removed.initialization = TopologyInitialization::RetireAtCut;
                retired.push(removed);
            }
        }
    }
    retired.extend(target.into_values());
    retired.sort_unstable_by(|left, right| {
        (&left.name, left.catalog_generation).cmp(&(&right.name, right.catalog_generation))
    });
    Ok(retired)
}

fn validate_future_state_inputs(
    name: &str,
    operator: Option<&crate::operator::capability::OperatorCapability>,
    dependencies: &[String],
    objects: &BTreeMap<String, ClusterTopologyObjectPlan>,
    graph: &PlannedTopologyGraph,
) -> Result<(), DbError> {
    if operator.is_some_and(|capability| match capability.state_class {
        OperatorStateClass::Stateless => capability.managed_state.is_some(),
        OperatorStateClass::VnodeKeyed | OperatorStateClass::GlobalSingleton => {
            capability.managed_state.is_none()
        }
        _ => true,
    }) {
        return Err(TopologyError::Unsupported(format!(
            "new stream '{name}' has no supported empty managed-state initialization contract"
        ))
        .into());
    }
    if operator.is_none_or(|capability| capability.managed_state.is_none()) {
        return Ok(());
    }
    let mut pending: Vec<_> = dependencies.iter().map(String::as_str).collect();
    let mut visited = std::collections::BTreeSet::new();
    while let Some(dependency) = pending.pop() {
        if !visited.insert(dependency) {
            continue;
        }
        let object = objects.get(dependency).ok_or_else(|| {
            TopologyError::Invalid("new managed state has an unresolved input".into())
        })?;
        if graph.source_input_modes.get(dependency)
            == Some(&laminar_connectors::connector::SourceInputMode::FullChangelog)
            || (object.transition == ClusterTopologyObjectTransition::Preserve
                && graph.schemas.get(dependency).is_some_and(|schema| {
                    schema
                        .field_with_name(laminar_core::changelog::WEIGHT_COLUMN)
                        .is_ok()
                }))
        {
            return Err(TopologyError::Unsupported(format!(
                "new managed stream '{name}' consumes changelog '{dependency}' without a cut baseline; empty state cannot apply retractions from before the cut"
            )).into());
        }
        pending.extend(object.dependencies.iter().map(String::as_str));
    }
    Ok(())
}

const fn state_contract_name(contract: ManagedStateContract) -> &'static str {
    match contract {
        ManagedStateContract::SqlAggregateV1 => "sql_aggregate_v1",
        ManagedStateContract::CoreWindowV1 => "core_window_v1",
        ManagedStateContract::BoundedIntervalJoinV3 => "bounded_interval_join_v3",
        ManagedStateContract::TemporalJoinV1 => "temporal_join_v1",
        ManagedStateContract::ProcessFunctionV1 => "process_function_v1",
        #[cfg(test)]
        ManagedStateContract::TestVnodeStateV1 => "test_vnode_state_v1",
    }
}
