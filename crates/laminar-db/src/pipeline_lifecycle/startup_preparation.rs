#[cfg(feature = "cluster")]
use super::RuntimeMode;
use super::{DbError, HashMap, LaminarDB};

impl LaminarDB {
    pub(super) fn build_registered_source_config(
        &self,
        source_name: &str,
        registration: &crate::connector_manager::SourceRegistration,
    ) -> Result<laminar_connectors::config::ConnectorConfig, DbError> {
        let mut config = crate::connector_manager::build_source_config(registration)?;
        if let Some(entry) = self.catalog.get_source(source_name) {
            config.set(
                "_arrow_schema".to_string(),
                crate::pipeline_callback::encode_arrow_schema(&entry.schema),
            );
        }
        Ok(config)
    }

    pub(super) async fn start_inner(&self) -> Result<(), DbError> {
        let runtime_shutdown = tokio_util::sync::CancellationToken::new();
        *self.runtime_shutdown.write() = runtime_shutdown.clone();
        if self.is_closed() {
            runtime_shutdown.cancel();
            return Err(DbError::Shutdown);
        }

        let (source_regs, sink_regs, stream_regs, table_regs, has_external) = {
            let mgr = self.connector_manager.lock();
            (
                mgr.sources().clone(),
                mgr.sinks().clone(),
                mgr.streams().clone(),
                mgr.tables().clone(),
                mgr.has_external_connectors(),
            )
        };

        for (name, reg) in &source_regs {
            tracing::debug!(source = %name, connector_type = ?reg.connector_type, "Registered source");
        }
        for (name, reg) in &sink_regs {
            tracing::debug!(sink = %name, connector_type = ?reg.connector_type, "Registered sink");
        }

        let startup_runtime = self.runtime_mode();

        let temporal_source_roles = self.validate_persisted_temporal_source_contracts(
            &source_regs,
            &sink_regs,
            &stream_regs,
            startup_runtime,
        )?;
        let ordered_interval_admissions = self
            .validate_persisted_interval_source_contracts(
                &source_regs,
                &sink_regs,
                &stream_regs,
                startup_runtime,
            )
            .await?;
        let mut registered_source_names = source_regs.keys().collect::<Vec<_>>();
        registered_source_names.sort_unstable();
        for source_name in registered_source_names {
            self.validate_registered_mutation_source_admission(
                source_name,
                &source_regs,
                &temporal_source_roles,
                &ordered_interval_admissions,
            )?;
        }

        let injected_cluster_checkpoint_store =
            self.validate_startup_durability(startup_runtime)?;

        // Freeze assignment publication before checkpoint initialization snapshots the registry.
        // The previous, narrower guard in `start_connector_pipeline` left a window where a watcher
        // could advance the registry after the coordinator captured its version but before runtime
        // launch, binding the new graph to the stale assignment.
        #[cfg(feature = "cluster")]
        let startup_assignment_guard = if startup_runtime == RuntimeMode::Cluster {
            Some(self.assignment_adoption_lock.lock().await)
        } else {
            None
        };

        let pipeline_identity = self
            .initialize_checkpointing(
                &source_regs,
                &sink_regs,
                &stream_regs,
                &table_regs,
                startup_runtime,
                injected_cluster_checkpoint_store,
            )
            .await?;

        if has_external || !stream_regs.is_empty() {
            tracing::info!(
                sources = source_regs.len(),
                sinks = sink_regs.len(),
                streams = stream_regs.len(),
                tables = table_regs.len(),
                has_external,
                "Starting pipeline"
            );
            self.start_connector_pipeline(
                source_regs,
                sink_regs,
                stream_regs,
                table_regs,
                has_external,
                pipeline_identity,
                temporal_source_roles,
                ordered_interval_admissions,
                runtime_shutdown,
            )
            .await?;
        } else {
            tracing::info!(
                sources = source_regs.len(),
                sinks = sink_regs.len(),
                "Starting in embedded (in-memory) mode — no streams"
            );
        }

        #[cfg(feature = "cluster")]
        drop(startup_assignment_guard);

        Ok(())
    }

    pub(crate) async fn revalidate_persisted_cluster_query_shapes(
        &self,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
    ) -> Result<bool, DbError> {
        let mut streams: Vec<_> = stream_regs.values().collect();
        streams.sort_by(|left, right| left.name.cmp(&right.name));
        let mut has_ownership_partitioned_state = false;
        for stream in streams {
            let plan = crate::ddl::PlannedStreamingQuery {
                emit_clause: stream.emit_clause.clone(),
                window_config: stream.window_config.clone(),
                order_config: stream.order_config.clone(),
                join_config: stream.join_config.clone(),
                has_analytic: stream.has_analytic,
                has_frame: stream.has_frame,
            };
            self.validate_interval_join_schema(&stream.name, &stream.query_sql, &plan)
                .await?;
            has_ownership_partitioned_state |= self
                .validate_cluster_query_shape(
                    "persisted stream",
                    &stream.name,
                    &stream.query_sql,
                    &plan,
                )
                .await?;
        }
        Ok(has_ownership_partitioned_state)
    }
}
