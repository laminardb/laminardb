use super::{
    admit_source_contract, admit_source_recovery_contract, admit_temporal_source_contract,
    exact_table_reference, schema_has_reserved_mutation_columns, Arc,
    ConnectorTaskFenceRegistration, DbError, FxHashMap, HashMap, LaminarDB, RuntimeMode,
    SourceInputMode, SourceTopology, TemporalSourceRole, TrackedSourceRegistration,
};

impl LaminarDB {
    pub(super) fn build_connector_operator_graph(
        &self,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
        table_regs: &HashMap<String, crate::connector_manager::TableRegistration>,
        changelog_carrying: &rustc_hash::FxHashSet<String>,
        ordered_interval_joins: &FxHashMap<
            String,
            [crate::operator::interval_join_input::BoundedJoinInputMode; 2],
        >,
        pipeline_identity: Option<&laminar_core::checkpoint::PipelineIdentity>,
    ) -> Result<crate::operator_graph::OperatorGraph, DbError> {
        use crate::operator_graph::OperatorGraph;

        #[cfg(not(feature = "cluster"))]
        let _ = pipeline_identity;
        let ctx = {
            use datafusion::execution::SessionStateBuilder;
            let mut session_config = laminar_sql::datafusion::base_session_config();
            if let Some(n) = self.pipeline_target_partitions {
                session_config = session_config.with_target_partitions(n);
            }
            let query_planner = Arc::clone(self.ctx.state().query_planner());
            let mut state_builder = SessionStateBuilder::new()
                .with_config(session_config)
                .with_default_features()
                .with_query_planner(query_planner);
            for rule in self.physical_optimizer_rules.iter() {
                state_builder = state_builder.with_physical_optimizer_rule(Arc::clone(rule));
            }
            let context =
                datafusion::prelude::SessionContext::new_with_state(state_builder.build());
            for rule in self.ctx.state().optimizers() {
                context.add_optimizer_rule(Arc::clone(rule));
            }
            context
        };
        laminar_sql::register_streaming_functions(&ctx);
        self.register_custom_functions_into(&ctx);

        let lookup_tables: Vec<(String, arrow::datatypes::SchemaRef)> = {
            let ts = self.table_store.read();
            ts.table_names()
                .into_iter()
                .filter_map(|name| {
                    let schema = ts.table_schema(&name)?;
                    Some((name, schema))
                })
                .collect()
        };
        // Record only tables that actually registered into `ctx`, so the graph's reference-table
        // set can't name a table the DataFusion context is missing (enrich detection would then
        // build SQL against a non-existent table).
        let mut reference_table_names = rustc_hash::FxHashSet::default();
        for (name, schema) in lookup_tables {
            let provider = crate::table_provider::ReferenceTableProvider::new(
                name.clone(),
                schema,
                self.table_store.clone(),
            );
            if let Err(e) = ctx.register_table(exact_table_reference(&name), Arc::new(provider)) {
                tracing::warn!(
                    table = %name,
                    error = %e,
                    "failed to register lookup table in operator graph context"
                );
            } else if !table_regs
                .get(&name)
                .is_some_and(|registration| registration.on_demand)
            {
                reference_table_names.insert(name);
            }
        }

        let mut graph = OperatorGraph::new(ctx);
        graph.set_ordered_interval_joins(ordered_interval_joins.clone());
        graph.set_key_group_count(self.checkpoint_key_groups());
        graph.set_temporal_join_idle_history_retention(
            self.config.temporal_join_idle_history_retention,
        );
        graph.set_lookup_registry(Arc::clone(&self.lookup_registry));
        graph.set_reference_tables(reference_table_names);
        if let Some(ref prom) = *self.engine_metrics.lock() {
            graph.set_metrics(Arc::clone(prom));
        }
        if let (Some(runtime), Some(handle)) = (&self.ai_runtime, &self.ai_handle) {
            graph.set_ai_runtime(Arc::clone(runtime), handle.clone());
        }

        #[cfg(feature = "cluster")]
        {
            let sender = self.shuffle_sender.lock().clone();
            let receiver = self.shuffle_receiver.lock().clone();
            let registry = self.vnode_registry.lock().clone();
            let controller = self.cluster_controller.lock().clone();
            if let (Some(sender), Some(receiver), Some(registry), Some(controller)) =
                (sender, receiver, registry, controller)
            {
                let self_id = laminar_core::state::NodeId(controller.instance_id().0);
                graph.set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
                    registry,
                    sender,
                    receiver,
                    self_id,
                });
                let pipeline_identity = pipeline_identity.cloned().ok_or_else(|| {
                    DbError::Checkpoint(
                        "[LDB-6051] cluster graph has no bound pipeline identity".into(),
                    )
                })?;
                graph.set_pipeline_identity(pipeline_identity);
                graph.set_pending_vnode_transition_handle(Arc::clone(
                    &self.pending_vnode_transition,
                ));
                graph.set_installed_vnode_state_handle(Arc::clone(&self.installed_vnode_state));
                graph.set_rotation_execution_fence(Arc::clone(&self.rotation_execution_fence));
            }
        }

        // The connector manager contains only externally configured sources. Plain SQL-created
        // sources are bridged directly from the catalog, but managed operators must still plan
        // against their schemas before source connectors and checkpoint recovery are built.
        for name in self.catalog.list_sources() {
            if let Some(entry) = self.catalog.get_source(&name) {
                graph.register_source_schema(name, entry.schema.clone());
            }
        }

        let partial_lookup_tables: rustc_hash::FxHashMap<String, Vec<String>> = table_regs
            .values()
            .filter(|r| r.on_demand)
            .filter_map(|r| {
                let schema = self.table_store.read().table_schema(&r.name)?;
                let cols = schema.fields().iter().map(|f| f.name().clone()).collect();
                Some((r.name.clone(), cols))
            })
            .collect();
        graph.set_partial_lookup_tables(partial_lookup_tables);
        graph.set_runtime_handle(
            self.ai_handle
                .clone()
                .unwrap_or_else(tokio::runtime::Handle::current),
        );

        // Seed changelog producers up front so consumer admission is independent of build order.
        graph.set_changelog_tables(changelog_carrying.clone());

        let mut ordered_streams: Vec<_> = stream_regs.values().collect();
        ordered_streams.sort_by(|left, right| left.name.cmp(&right.name));
        for reg in ordered_streams {
            graph.add_query(
                reg.name.clone(),
                reg.query_sql.clone(),
                reg.emit_clause.clone(),
                reg.window_config.clone(),
                reg.order_config.clone(),
                reg.join_config.clone(),
                reg.incremental,
            );
        }
        graph.take_build_errors()?;

        Ok(graph)
    }

    pub(super) fn build_pipeline_sources(
        &self,
        source_regs: &HashMap<String, crate::connector_manager::SourceRegistration>,
        temporal_source_roles: &FxHashMap<String, TemporalSourceRole>,
        ordered_interval_source_modes: &FxHashMap<String, SourceInputMode>,
        checkpointing_enabled: bool,
        runtime_mode: RuntimeMode,
        prom_registry: Option<&Arc<prometheus::Registry>>,
    ) -> Result<Vec<TrackedSourceRegistration>, DbError> {
        use crate::pipeline::SourceRegistration;
        use laminar_connectors::connector::SourceConnector as _;
        let mut sources: Vec<TrackedSourceRegistration> = Vec::new();
        for (name, reg) in source_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let source_entry = self.catalog.get_source(name);
            let config = self.build_registered_source_config(name, reg)?;

            let source = self
                .connector_registry
                .create_source(&config, prom_registry)
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Cannot create source '{}' (type '{}'): {e}",
                        name,
                        config.connector_type()
                    ))
                })?;
            let task_fence = ConnectorTaskFenceRegistration::capture_registered(
                Arc::<str>::from(format!("source:{name}")),
                source.terminal_task_tracker(),
                &self.owned_connector_task_fences,
            );
            let mut source = TrackedSourceRegistration::from_captured(
                SourceRegistration {
                    name: name.clone(),
                    connector: source,
                    config,
                    assignment_scoped: false,
                    position: laminar_connectors::connector::SourcePosition::Initial,
                },
                task_fence,
            )?;
            if let Some(entry) = source_entry.as_ref() {
                source =
                    source.with_admitted_schema(entry.schema.clone(), entry.primary_key.clone())?;
            }
            let contract = source.contract();
            let has_primary_key = source_entry
                .as_ref()
                .is_some_and(|entry| !entry.primary_key.is_empty());
            let has_reserved_mutation_columns = source_entry
                .as_ref()
                .is_some_and(|entry| schema_has_reserved_mutation_columns(entry.schema.as_ref()));
            let temporal_role = temporal_source_roles.get(name).copied();
            let ordered_interval_mode = ordered_interval_source_modes.get(name).copied();
            let admission = if let Some(mode) = ordered_interval_mode {
                if mode == contract.input_mode {
                    admit_source_recovery_contract(
                        contract,
                        self.config.delivery_guarantee,
                        checkpointing_enabled,
                        runtime_mode,
                    )
                } else {
                    Err("bounded interval source contract changed after startup admission")
                }
            } else if let Some(role) = temporal_role {
                admit_temporal_source_contract(
                    contract,
                    role,
                    has_primary_key,
                    has_reserved_mutation_columns,
                    self.config.delivery_guarantee,
                    checkpointing_enabled,
                    runtime_mode,
                )
            } else {
                admit_source_contract(
                    contract,
                    has_primary_key,
                    has_reserved_mutation_columns,
                    self.config.delivery_guarantee,
                    checkpointing_enabled,
                    runtime_mode,
                )
            };
            admission.map_err(|reason| {
                DbError::Config(format!(
                    "source '{name}' is not admissible in {runtime_mode:?} mode with {} delivery: \
                     {reason} (contract: {contract:?})",
                    self.config.delivery_guarantee
                ))
            })?;
            if matches!(temporal_role, Some(TemporalSourceRole::Right))
                && contract.input_mode == SourceInputMode::KeyedUpsert
            {
                source = source.with_temporal_right_mutations();
            }
            if let Some(mode) = ordered_interval_mode {
                source = source.with_ordered_interval_input_mode(mode)?;
            }
            let assignment_scoped = cfg!(feature = "cluster")
                && runtime_mode == RuntimeMode::Cluster
                && contract.topology == SourceTopology::Splittable;
            source.assignment_scoped = assignment_scoped;
            #[cfg(feature = "cluster")]
            if assignment_scoped {
                let registry = self.vnode_registry.lock().clone().ok_or_else(|| {
                    DbError::Config(format!("cluster source '{name}' has no vnode registry"))
                })?;
                let self_id = self
                    .cluster_controller
                    .lock()
                    .as_ref()
                    .map(|controller| laminar_core::state::NodeId(controller.instance_id().0))
                    .ok_or_else(|| {
                        DbError::Config(format!(
                            "cluster source '{name}' has no cluster controller identity"
                        ))
                    })?;
                source
                    .connector
                    .set_vnode_assignment(name, registry, self_id)
                    .map_err(|error| {
                        DbError::Config(format!(
                            "source '{name}' rejected cluster vnode assignment: {error}"
                        ))
                    })?;
            }
            sources.push(source);
        }

        let bridged_names: rustc_hash::FxHashSet<String> =
            sources.iter().map(|s| s.name.clone()).collect();
        for (name, reg) in source_regs {
            if reg.connector_type.is_some() {
                continue;
            }
            if let Some(entry) = self.catalog.get_source(name) {
                let subscription = entry.sink.subscribe();
                let connector = crate::catalog_connector::CatalogSourceConnector::new(
                    subscription,
                    entry.schema.clone(),
                    entry.data_notify(),
                );
                let task_fence = ConnectorTaskFenceRegistration::capture_registered(
                    Arc::<str>::from(format!("source:{name}")),
                    connector.terminal_task_tracker(),
                    &self.owned_connector_task_fences,
                );
                let config = laminar_connectors::config::ConnectorConfig::new("catalog-bridge");
                let source = TrackedSourceRegistration::from_captured(
                    SourceRegistration {
                        name: name.clone(),
                        connector: Box::new(connector),
                        config,
                        assignment_scoped: false,
                        position: laminar_connectors::connector::SourcePosition::Initial,
                    },
                    task_fence,
                )?
                .with_admitted_schema(entry.schema.clone(), entry.primary_key.clone())?;
                let contract = source.contract();
                admit_source_contract(
                    contract,
                    !entry.primary_key.is_empty(),
                    schema_has_reserved_mutation_columns(entry.schema.as_ref()),
                    self.config.delivery_guarantee,
                    checkpointing_enabled,
                    runtime_mode,
                )
                .map_err(|reason| {
                    DbError::Config(format!(
                        "source '{name}' is not admissible in {runtime_mode:?} mode with {} \
                         delivery: {reason} (contract: {contract:?})",
                        self.config.delivery_guarantee
                    ))
                })?;
                sources.push(source);
            }
        }
        for name in self.catalog.list_sources() {
            if bridged_names.contains(&name) || source_regs.contains_key(&name) {
                continue;
            }
            if let Some(entry) = self.catalog.get_source(&name) {
                let subscription = entry.sink.subscribe();
                let connector = crate::catalog_connector::CatalogSourceConnector::new(
                    subscription,
                    entry.schema.clone(),
                    entry.data_notify(),
                );
                let task_fence = ConnectorTaskFenceRegistration::capture_registered(
                    Arc::<str>::from(format!("source:{name}")),
                    connector.terminal_task_tracker(),
                    &self.owned_connector_task_fences,
                );
                let config = laminar_connectors::config::ConnectorConfig::new("catalog-bridge");
                let source = TrackedSourceRegistration::from_captured(
                    SourceRegistration {
                        name: name.clone(),
                        connector: Box::new(connector),
                        config,
                        assignment_scoped: false,
                        position: laminar_connectors::connector::SourcePosition::Initial,
                    },
                    task_fence,
                )?
                .with_admitted_schema(entry.schema.clone(), entry.primary_key.clone())?;
                let contract = source.contract();
                admit_source_contract(
                    contract,
                    !entry.primary_key.is_empty(),
                    schema_has_reserved_mutation_columns(entry.schema.as_ref()),
                    self.config.delivery_guarantee,
                    checkpointing_enabled,
                    runtime_mode,
                )
                .map_err(|reason| {
                    DbError::Config(format!(
                        "source '{name}' is not admissible in {runtime_mode:?} mode with {} \
                         delivery: {reason} (contract: {contract:?})",
                        self.config.delivery_guarantee
                    ))
                })?;
                sources.push(source);
            }
        }
        Ok(sources)
    }
}
