use super::{
    admit_sink, open_prepared_sinks, Arc, CheckpointStorageScope, ConnectorTaskFenceRegistration,
    DbError, DeliveryGuarantee, HashMap, LaminarDB, PipelineSinkSetup, PreparedSink, RuntimeMode,
    SinkAdmissionContext, SinkContract, TrackedSourceRegistration,
};

impl LaminarDB {
    pub(super) async fn prepare_pipeline_sinks(
        &self,
        sources: &[TrackedSourceRegistration],
        sink_regs: &HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_output_schemas: &HashMap<String, arrow_schema::SchemaRef>,
        changelog_carrying: &rustc_hash::FxHashSet<String>,
        runtime_mode: RuntimeMode,
        checkpointing_enabled: bool,
        pipeline_checkpoint_timeout: std::time::Duration,
        prom_registry: Option<&Arc<prometheus::Registry>>,
    ) -> Result<PipelineSinkSetup, DbError> {
        use crate::connector_manager::build_sink_config;
        let (sink_event_tx, sink_event_rx) =
            laminar_core::streaming::channel::channel::<crate::sink_task::SinkEvent>(
                crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY,
            );

        let mut prepared_sinks = Vec::new();
        for (name, reg) in sink_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let mut config = build_sink_config(reg, self.config.delivery_guarantee)?;
            let upstream_schema = stream_output_schemas.get(&reg.input).cloned().or_else(|| {
                self.catalog
                    .get_source(&reg.input)
                    .map(|e| e.schema.clone())
            });
            if let Some(schema) = upstream_schema {
                let schema_str = crate::pipeline_callback::encode_arrow_schema(&schema);
                config.set("_arrow_schema".to_string(), schema_str);
            }
            let sink = self
                .connector_registry
                .create_sink(&config, prom_registry)
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Cannot create sink '{}' (type '{}'): {e}",
                        name,
                        config.connector_type()
                    ))
                })?;
            let task_fence = ConnectorTaskFenceRegistration::capture_registered(
                Arc::<str>::from(format!("sink:{name}")),
                sink.terminal_task_tracker(),
                &self.owned_connector_task_fences,
            );

            let carries_changelog = changelog_carrying.contains(&reg.input);
            #[cfg(feature = "cluster")]
            let injected_shared_store = self.cluster_checkpoint_object_store().is_some();
            #[cfg(not(feature = "cluster"))]
            let injected_shared_store = false;
            let checkpoint_storage_scope = if self.config.checkpoint.is_none() {
                CheckpointStorageScope::Volatile
            } else if injected_shared_store {
                CheckpointStorageScope::ClusterShared
            } else {
                self.config.object_store_url.as_deref().map_or(
                    CheckpointStorageScope::NodeDurable,
                    CheckpointStorageScope::for_url,
                )
            };
            let (contract, configured_timeout) = admit_sink(
                sink.as_ref(),
                SinkAdmissionContext {
                    config: &config,
                    name,
                    input: &reg.input,
                    delivery: self.config.delivery_guarantee,
                    runtime: runtime_mode,
                    carries_changelog,
                    checkpointing_enabled,
                    checkpoint_storage_scope,
                },
            )?;
            let write_timeout = configured_timeout.map_or(
                sink.suggested_write_timeout(),
                std::time::Duration::from_millis,
            );
            if write_timeout.is_zero() {
                return Err(DbError::Connector(format!(
                    "sink '{name}': write_timeout must be > 0 \
                     (check 'sink.write.timeout.ms' or the sink's \
                     suggested_write_timeout)"
                )));
            }
            let flush_interval = sink.flush_interval();
            if flush_interval.is_zero() {
                return Err(DbError::Connector(format!(
                    "sink '{name}': flush_interval must be > 0"
                )));
            }
            prepared_sinks.push(PreparedSink {
                name: name.clone(),
                connector: sink,
                config,
                filter_expr: reg.filter_expr.clone(),
                input: reg.input.clone(),
                contract,
                expects_changelog: carries_changelog,
                write_timeout,
                flush_interval,
                requires_recovery_on_error: contract.is_checkpoint_committable()
                    || self.config.delivery_guarantee != DeliveryGuarantee::BestEffort
                    || runtime_mode == RuntimeMode::Cluster,
                task_fence,
            });
        }

        #[cfg(feature = "cluster")]
        let callback_controller = self.cluster_controller.lock().clone();
        #[cfg(feature = "cluster")]
        let sink_process_authority = if runtime_mode == RuntimeMode::Cluster {
            let controller = callback_controller.clone().ok_or_else(|| {
                DbError::Config(
                    "cluster sink runtime requires a cluster controller with process lease authority"
                        .into(),
                )
            })?;
            if controller.process_lease_deadline().is_none() {
                return Err(DbError::Config(
                    "cluster sink runtime requires one shared process lease deadline before open"
                        .into(),
                ));
            }
            Some(controller)
        } else {
            None
        };

        // Opening is one atomic startup stage: a slow connector consumes the remaining shared
        // checkpoint-derived budget rather than receiving a fresh timeout of its own. Cluster
        // opens use the exact authority later installed in the actor and callback.
        open_prepared_sinks(
            &mut prepared_sinks,
            pipeline_checkpoint_timeout,
            #[cfg(feature = "cluster")]
            sink_process_authority.as_deref(),
        )
        .await?;

        let mut sinks: Vec<(
            String,
            crate::sink_task::SinkTaskHandle,
            Option<String>,
            String, // input stream name (FROM clause target)
            SinkContract,
            bool, // admitted input is a changelog and must carry canonical weight
        )> = Vec::with_capacity(prepared_sinks.len());
        for prepared in prepared_sinks {
            let PreparedSink {
                name,
                connector,
                filter_expr,
                input,
                contract,
                expects_changelog,
                write_timeout,
                flush_interval,
                requires_recovery_on_error,
                task_fence,
                config: _,
            } = prepared;
            let terminal_tasks = task_fence.tracker();
            let sink_id: std::sync::Arc<str> = std::sync::Arc::from(name.as_str());
            let handle =
                crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
                    name: name.clone(),
                    sink_id,
                    connector,
                    contract,
                    requires_recovery_on_error,
                    channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
                    flush_interval,
                    write_timeout,
                    event_tx: sink_event_tx.clone(),
                    terminal_tasks,
                    #[cfg(feature = "cluster")]
                    process_authority: sink_process_authority.clone(),
                });
            {
                let mut owned = self.owned_sink_handles.lock();
                debug_assert!(!owned.iter().any(|known| known.same_actor(&handle)));
                owned.push(handle.clone());
            }
            sinks.push((
                name,
                handle,
                filter_expr,
                input,
                contract,
                expects_changelog,
            ));
            task_fence.handoff();
        }
        drop(sink_event_tx);

        {
            let mut guard = self.coordinator.lock().await;
            if let Some(coord) = guard.as_mut() {
                coord.set_assignment_scoped_sources(
                    sources
                        .iter()
                        .filter(|source| source.assignment_scoped)
                        .map(|source| source.name.clone()),
                );
                for (name, handle, _, _, _, _) in &sinks {
                    coord.register_sink(name.clone(), handle.clone());
                }
            }
        }

        #[cfg(feature = "cluster")]
        {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                // Resolve any interrupted sink epoch before coordinated recovery opens connectors.
                if runtime_mode == RuntimeMode::Cluster {
                    coord.reconcile_sink_open_witness().await?;
                }
            }
        }

        Ok(PipelineSinkSetup {
            sinks,
            sink_event_rx,
            #[cfg(feature = "cluster")]
            callback_controller,
        })
    }
}
