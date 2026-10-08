//! Effect-free reuse of startup's graph, schema and connector admission checks.
//!
//! Called only on a private Created database with no authority, actors or transport installed.
//! No connector is opened, polled, bound to a sink epoch, or asked to discover source positions.

use std::collections::BTreeMap;
use std::sync::Arc;

use laminar_connectors::connector::SourceInputMode;
use laminar_core::checkpoint::object_store_builder::CheckpointStorageScope;
use laminar_core::cluster::control::TopologyError;

use super::{
    admit_sink, admit_source_contract, admit_source_recovery_contract, SinkAdmissionContext,
};
use crate::db::{DbState, LaminarDB, RuntimeMode};
use crate::error::DbError;
use crate::operator::capability::OperatorCapability;
use crate::pipeline_identity::compatibility_digest;

pub(crate) struct PlannedTopologyGraph {
    pub(crate) operators: BTreeMap<String, OperatorCapability>,
    pub(crate) schemas: BTreeMap<String, arrow_schema::SchemaRef>,
    pub(crate) connector_sha256: BTreeMap<String, String>,
    pub(crate) source_input_modes: BTreeMap<String, SourceInputMode>,
}

impl LaminarDB {
    pub(crate) async fn plan_topology_graph(&self) -> Result<PlannedTopologyGraph, DbError> {
        self.compile_topology_graph()
            .await
            .map(|(description, _)| description)
    }

    /// Reuse the same effect-free compiler, retaining its unstarted graph for private restore.
    pub(crate) async fn compile_topology_graph(
        &self,
    ) -> Result<(PlannedTopologyGraph, crate::operator_graph::OperatorGraph), DbError> {
        self.compile_topology_graph_inner(None).await
    }

    pub(crate) async fn compile_topology_restore_graph(
        &self,
        input: &laminar_core::cluster::control::TopologyRestoreInput,
        scope: crate::operator::sql_query::ClusterShuffleConfig,
    ) -> Result<(PlannedTopologyGraph, crate::operator_graph::OperatorGraph), DbError> {
        self.compile_topology_graph_inner(Some((input, scope)))
            .await
    }

    async fn compile_topology_graph_inner(
        &self,
        restore: Option<(
            &laminar_core::cluster::control::TopologyRestoreInput,
            crate::operator::sql_query::ClusterShuffleConfig,
        )>,
    ) -> Result<(PlannedTopologyGraph, crate::operator_graph::OperatorGraph), DbError> {
        if DbState::load(&self.state) != DbState::Created
            || self.topology_planning_ownership_scope.is_none()
            || self.cluster_controller.lock().is_some()
            || self.catalog_manifest_store.lock().is_some()
            || self.shuffle_sender.lock().is_some()
            || self.shuffle_receiver.lock().is_some()
        {
            return Err(TopologyError::Invalid(
                "candidate graph planning requires an isolated Created catalog".into(),
            )
            .into());
        }
        let (sources, sinks, mut streams, tables) = {
            let manager = self.connector_manager.lock();
            (
                manager.sources().clone(),
                manager.sinks().clone(),
                manager.streams().clone(),
                manager.tables().clone(),
            )
        };
        let temporal = self.validate_persisted_temporal_source_contracts(
            &sources,
            &sinks,
            &streams,
            RuntimeMode::Cluster,
        )?;
        let interval = self
            .validate_persisted_interval_source_contracts(
                &sources,
                &sinks,
                &streams,
                RuntimeMode::Cluster,
            )
            .await?;
        self.revalidate_persisted_cluster_query_shapes(&streams)
            .await?;
        let mut resolved = super::resolve_stream_output_schemas(
            &self.ctx,
            &streams,
            &rustc_hash::FxHashSet::default(),
            &interval.joins,
        )
        .await?;
        for registration in self.connector_manager.lock().process_functions().values() {
            if self
                .catalog
                .get_stream_entry(&registration.output_name)
                .is_none()
            {
                continue;
            }
            resolved.schemas.insert(
                registration.output_name.clone(),
                Arc::clone(&registration.descriptor.output_schema),
            );
        }
        let mut connector_sha256 = BTreeMap::new();
        let mut source_input_modes = BTreeMap::new();
        let mut schemas: BTreeMap<_, _> = resolved
            .schemas
            .iter()
            .map(|(name, schema)| (name.clone(), Arc::clone(schema)))
            .collect();
        // Iterate canonical names for deterministic failures as well as deterministic hashes.
        let mut source_names = self.catalog.list_sources();
        source_names.sort_unstable();
        for name in source_names {
            self.validate_registered_mutation_source_admission(
                &name, &sources, &temporal, &interval,
            )?;
            let source = self.catalog.get_source(&name).ok_or_else(|| {
                TopologyError::Invalid(format!(
                    "source '{name}' disappeared from candidate catalog"
                ))
            })?;
            let registration = sources.get(&name).ok_or_else(|| {
                TopologyError::Unsupported(format!(
                    "source '{name}' has no replayable connector; catalog ingress has no durable migration start boundary"
                ))
            })?;
            let config = self.build_registered_source_config(&name, registration)?;
            let connector = self.connector_registry.create_source(&config, None)?;
            let contract = connector.contract(&config)?;
            let result = if contract.input_mode == SourceInputMode::AppendOnly {
                admit_source_contract(
                    contract,
                    !source.primary_key.is_empty(),
                    crate::catalog::schema_has_reserved_mutation_columns(&source.schema),
                    self.config.delivery_guarantee,
                    self.config.checkpoint.is_some(),
                    RuntimeMode::Cluster,
                )
            } else {
                // Role-specific admission above certifies the mutation consumer and schema.
                admit_source_recovery_contract(
                    contract,
                    self.config.delivery_guarantee,
                    self.config.checkpoint.is_some(),
                    RuntimeMode::Cluster,
                )
            };
            result.map_err(|reason| {
                TopologyError::Unsupported(format!("source '{name}': {reason}"))
            })?;
            connector_sha256.insert(
                name.clone(),
                compatibility_digest(&(
                    "laminardb-topology-source-contract-v1",
                    self.connector_registry
                        .source_info(config.connector_type())
                        .map(|info| (info.name, info.version))
                        .ok_or_else(|| {
                            TopologyError::Invalid(format!(
                                "source '{name}' has no registered implementation metadata"
                            ))
                        })?,
                    contract,
                    connector.cancellation_policy(),
                ))?,
            );
            source_input_modes.insert(name.clone(), contract.input_mode);
            schemas.insert(name, Arc::clone(&source.schema));
        }
        connector_sha256.extend(
            self.plan_topology_sinks(&sinks, &schemas, &resolved)
                .await?,
        );
        if let Some((input, _)) = &restore {
            self.bind_topology_subscriptions(&mut streams, input, &resolved.schemas)?;
        }
        let graph = if let Some((input, scope)) = restore {
            self.build_topology_restore_operator_graph(
                &streams,
                &tables,
                &resolved.changelog_carrying,
                &interval.joins,
                &input.descriptor().target_pipeline,
                scope,
            )?
        } else {
            self.build_connector_operator_graph(
                &streams,
                &tables,
                &resolved.changelog_carrying,
                &interval.joins,
                None,
            )?
        };
        self.initialize_topology_graph(
            graph,
            &resolved.schemas,
            schemas,
            connector_sha256,
            source_input_modes,
        )
        .await
    }

    async fn plan_topology_sinks(
        &self,
        sinks: &std::collections::HashMap<String, crate::connector_manager::SinkRegistration>,
        schemas: &BTreeMap<String, arrow_schema::SchemaRef>,
        resolved: &super::output_schema::ResolvedStreamOutputs,
    ) -> Result<BTreeMap<String, String>, DbError> {
        let mut connector_sha256 = BTreeMap::new();
        let mut sink_names: Vec<_> = sinks.keys().collect();
        sink_names.sort_unstable();
        for name in sink_names {
            let registration = &sinks[name];
            let schema = schemas.get(&registration.input).ok_or_else(|| {
                TopologyError::Invalid(format!(
                    "sink '{name}' input '{}' has no resolved schema",
                    registration.input
                ))
            })?;
            if registration.connector_type.is_none() {
                return Err(TopologyError::Unsupported(format!(
                    "sink '{name}' has no durable connector; catalog-only output is not an external migration sink"
                )).into());
            }
            let mut config = crate::connector_manager::build_sink_config(
                registration,
                self.config.delivery_guarantee,
            )?;
            config.set(
                "_arrow_schema",
                crate::pipeline_callback::encode_arrow_schema(schema),
            );
            let connector = self.connector_registry.create_sink(&config, None)?;
            let (contract, _) = admit_sink(
                connector.as_ref(),
                SinkAdmissionContext {
                    config: &config,
                    name,
                    input: &registration.input,
                    delivery: self.config.delivery_guarantee,
                    runtime: RuntimeMode::Cluster,
                    carries_changelog: resolved.changelog_carrying.contains(&registration.input),
                    checkpointing_enabled: self.config.checkpoint.is_some(),
                    checkpoint_storage_scope: CheckpointStorageScope::ClusterShared,
                },
            )?;
            if connector.suggested_write_timeout().is_zero() || connector.flush_interval().is_zero()
            {
                return Err(TopologyError::Unsupported(format!(
                    "sink '{name}' requires nonzero runtime deadlines"
                ))
                .into());
            }
            if let Some(filter) = &registration.filter_expr {
                if resolved.changelog_carrying.contains(&registration.input) {
                    if crate::sql_analysis::predicate_references_weight(filter) {
                        return Err(TopologyError::Unsupported(format!(
                            "sink '{name}' filter references an engine-owned changelog column"
                        ))
                        .into());
                    }
                    crate::filter_compile::compile_replay_immutable(&self.ctx, filter, schema)
                        .await?;
                } else {
                    crate::filter_compile::compile(&self.ctx, filter, schema).await?;
                }
            }
            connector_sha256.insert(
                name.clone(),
                compatibility_digest(&(
                    "laminardb-topology-sink-contract-v1",
                    self.connector_registry
                        .sink_info(config.connector_type())
                        .map(|info| (info.name, info.version))
                        .ok_or_else(|| {
                            TopologyError::Invalid(format!(
                                "sink '{name}' has no registered implementation metadata"
                            ))
                        })?,
                    contract,
                    connector.cancellation_policy(),
                ))?,
            );
        }
        Ok(connector_sha256)
    }

    async fn initialize_topology_graph(
        &self,
        mut graph: crate::operator_graph::OperatorGraph,
        stream_schemas: &std::collections::HashMap<String, arrow_schema::SchemaRef>,
        schemas: BTreeMap<String, arrow_schema::SchemaRef>,
        connector_sha256: BTreeMap<String, String>,
        source_input_modes: BTreeMap<String, SourceInputMode>,
    ) -> Result<(PlannedTopologyGraph, crate::operator_graph::OperatorGraph), DbError> {
        for (name, schema) in stream_schemas {
            graph.register_intermediate_schema(name, schema);
        }
        let budget = self
            .config
            .pipeline_max_managed_state_bytes
            .ok_or_else(|| {
                TopologyError::Invalid("candidate has no resolved managed-state budget".into())
            })?;
        if budget == 0 {
            return Err(
                TopologyError::Invalid("candidate managed-state budget is zero".into()).into(),
            );
        }
        graph.set_max_managed_state_bytes(budget);
        // Only empty managed state is constructed. Historical state stays solely in the active
        // graph and its cut. Drop this graph before compiling the next candidate generation.
        let graph = graph.initialize_managed_state().await?;
        Ok((
            PlannedTopologyGraph {
                operators: graph.topology_operator_contracts()?,
                schemas,
                connector_sha256,
                source_input_modes,
            },
            graph,
        ))
    }
}
