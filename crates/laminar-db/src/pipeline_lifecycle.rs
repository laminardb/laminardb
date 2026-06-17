//! Pipeline lifecycle: start, close, shutdown.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use laminar_core::streaming;
use rustc_hash::FxHashMap;

use crate::db::{DbState, LaminarDB, SourceWatermarkState};
use crate::error::DbError;

/// Resolve a query's output schema by planning it. On `ASOF JOIN` failure,
/// retries with the schema-equivalent rewrite. Returns `None` if the query
/// still can't be planned (e.g. a dependency isn't registered yet).
pub(crate) async fn plan_output_schema(
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
) -> Option<arrow_schema::SchemaRef> {
    let plan = if let Ok(plan) = ctx.state().create_logical_plan(sql).await {
        plan
    } else {
        let rewritten = crate::sql_analysis::rewrite_asof_joins_for_planning(sql)?;
        ctx.state().create_logical_plan(&rewritten).await.ok()?
    };
    let fields: Vec<_> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| (**f).clone())
        .collect();
    Some(Arc::new(arrow_schema::Schema::new(fields)))
}

async fn resolve_stream_output_schemas(
    ctx: &datafusion::prelude::SessionContext,
    stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
) -> Result<HashMap<String, arrow_schema::SchemaRef>, DbError> {
    use datafusion::datasource::empty::EmptyTable;

    let mut out: HashMap<String, arrow_schema::SchemaRef> =
        HashMap::with_capacity(stream_regs.len());
    let mut pending: Vec<&crate::connector_manager::StreamRegistration> =
        stream_regs.values().collect();
    let mut placeholders: Vec<String> = Vec::new();

    let result: Result<(), DbError> = async {
        while !pending.is_empty() {
            let mut next: Vec<&crate::connector_manager::StreamRegistration> = Vec::new();
            let mut progressed = false;
            for reg in pending {
                let Some(schema) = plan_output_schema(ctx, &reg.query_sql).await else {
                    next.push(reg);
                    continue;
                };

                if !ctx.table_exist(&reg.name).unwrap_or(false) {
                    ctx.register_table(&reg.name, Arc::new(EmptyTable::new(schema.clone())))
                        .map_err(|e| {
                            DbError::Pipeline(format!(
                                "could not register placeholder for stream '{}': {e}",
                                reg.name
                            ))
                        })?;
                    placeholders.push(reg.name.clone());
                }
                out.insert(reg.name.clone(), schema);
                progressed = true;
            }

            if !progressed {
                let mut unresolved: Vec<&str> = next.iter().map(|r| r.name.as_str()).collect();
                unresolved.sort_unstable();
                // For ASOF joins, report the rewritten-plan error (the raw planner
                // just says "AsOf unsupported", which masks the real blocker).
                let sql = &next[0].query_sql;
                let err = match crate::sql_analysis::rewrite_asof_joins_for_planning(sql) {
                    Some(rewritten) => ctx.state().create_logical_plan(&rewritten).await.err(),
                    None => ctx.state().create_logical_plan(sql).await.err(),
                }
                .map_or_else(|| "unknown error".to_string(), |e| e.to_string());
                return Err(DbError::Pipeline(format!(
                    "unresolvable stream dependency among [{}]: {err}",
                    unresolved.join(", ")
                )));
            }
            pending = next;
        }
        Ok(())
    }
    .await;

    for name in &placeholders {
        let _ = ctx.deregister_table(name);
    }

    result.map(|()| out)
}

impl LaminarDB {
    /// Shut down the database gracefully.
    pub fn close(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Replay demoted vnode partials into the operator graph on restart.
    ///
    /// Fails loud on any unrecoverable vnode: source offsets are already staged,
    /// so resuming with empty state would silently corrupt that aggregate.
    #[cfg(feature = "state-tier")]
    async fn rehydrate_cold_vnodes(
        &self,
        graph: &mut crate::operator_graph::OperatorGraph,
        cold_map: &[(String, Vec<u32>)],
    ) -> Result<(), DbError> {
        let Some(backend) = self.state_backend.lock().clone() else {
            return Err(DbError::Checkpoint(
                "[LDB-6030] demoted vnodes recorded but no state backend is \
                 wired — cannot recover them on restart"
                    .to_string(),
            ));
        };
        let mut all_cold: Vec<u32> = cold_map
            .iter()
            .flat_map(|(_, vs)| vs.iter().copied())
            .collect();
        all_cold.sort_unstable();
        all_cold.dedup();
        let rehy = crate::recovery_manager::VnodeRehydrator::new(backend.as_ref())
            .rehydrate(&all_cold)
            .await;

        let (mut applied, mut lost) = (0usize, 0usize);
        for (op_name, cold_vnodes) in cold_map {
            for &v in cold_vnodes {
                let Some(partial_bytes) = rehy.restored.get(&v) else {
                    tracing::error!(operator = %op_name, vnode = v, "demoted-vnode partial missing on restart");
                    lost += 1;
                    continue;
                };
                let partial = match crate::vnode_partial::VnodePartial::decode(partial_bytes) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!(operator = %op_name, vnode = v, error = %e, "demoted-vnode partial decode failed");
                        lost += 1;
                        continue;
                    }
                };
                // Absence from the partial means the operator had no groups in this vnode.
                if let Some((_, slice)) = partial.operators.iter().find(|(n, _)| n == op_name) {
                    match graph.apply_vnode_slice(op_name, v, slice) {
                        Ok(()) => applied += 1,
                        Err(e) => {
                            tracing::error!(operator = %op_name, vnode = v, error = %e, "demoted-vnode apply failed");
                            lost += 1;
                        }
                    }
                }
            }
        }
        if lost > 0 {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6030] {lost} demoted vnode slice(s) unrecoverable on restart \
                 — refusing to start with staged source offsets and lost state \
                 (see per-vnode errors above)"
            )));
        }
        tracing::info!(applied, "rehydrated demoted vnodes from durable partials");
        Ok(())
    }

    /// Returns `true` if the database has been shut down.
    pub fn is_closed(&self) -> bool {
        self.shutdown.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Connector add/remove DDL can't take effect while serving — connectors are
    /// built only at `start()`. `Starting` is allowed so catalog-manifest replay
    /// (which runs before `start_inner` builds connectors) is picked up.
    pub(crate) fn connector_ddl_rejected(&self) -> bool {
        matches!(
            DbState::load(&self.state),
            DbState::Running | DbState::ShuttingDown
        )
    }

    /// Start the streaming pipeline. Idempotent if already running. On failure
    /// (or recovering from `Faulted`) it rebuilds from the surviving catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the pipeline cannot be started.
    pub async fn start(&self) -> Result<(), DbError> {
        match DbState::load(&self.state) {
            DbState::Running | DbState::Starting => return Ok(()),
            DbState::Stopped => {
                return Err(DbError::InvalidOperation(
                    "Cannot start a stopped pipeline. Create a new LaminarDB instance.".into(),
                ));
            }
            DbState::ShuttingDown => {
                return Err(DbError::InvalidOperation(
                    "cannot start pipeline: shutdown/stop in progress".into(),
                ));
            }
            // Faulted is recoverable — fall through and rebuild. Created is the
            // fresh-start path.
            DbState::Created | DbState::Faulted => {}
        }

        DbState::Starting.store(&self.state);
        // Clear on entry, not after start_inner — otherwise a panic during this
        // startup (watcher → Faulted + reason) would be immediately overwritten.
        *self.last_fault.lock() = None;

        {
            let mut guard = self.engine_metrics.lock();
            if guard.is_none() {
                *guard = Some(Arc::new(crate::engine_metrics::EngineMetrics::new(
                    &prometheus::Registry::new(),
                )));
            }
        }

        #[cfg(feature = "cluster")]
        self.restore_catalog_from_manifest().await;

        // Drain a shutdown permit a prior fault's `notify_one()` left with no
        // waiter, so the new coordinator's `notified()` doesn't fire at once.
        tokio::select! {
            biased;
            () = self.shutdown_signal.notified() => {}
            () = std::future::ready(()) => {}
        }

        match self.start_inner().await {
            // CAS, not store: don't clobber a Faulted set by the watcher if the
            // compute thread already panicked during startup.
            Ok(()) => {
                let _ = DbState::compare_exchange(DbState::Starting, DbState::Running, &self.state);
                Ok(())
            }
            Err(e) => {
                // Reset so a retry re-runs startup rather than silently returning Ok.
                DbState::Created.store(&self.state);
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn start_inner(&self) -> Result<(), DbError> {
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

        if let Some(ref cp_config) = self.config.checkpoint {
            use crate::checkpoint_coordinator::{
                CheckpointConfig as CkpConfig, CheckpointCoordinator,
            };

            let max_retained = cp_config.max_retained.unwrap_or(3);
            let vnode_count = self.vnode_registry.lock().as_ref().map_or(
                laminar_core::storage::checkpoint_manifest::DEFAULT_VNODE_COUNT,
                |r| u16::try_from(r.vnode_count()).unwrap_or(u16::MAX),
            );

            let data_dir = cp_config
                .data_dir
                .clone()
                .or_else(|| self.config.storage_dir.clone())
                .unwrap_or_else(|| std::path::PathBuf::from("./data"));

            let (store, decision_backing): (
                Box<dyn laminar_core::storage::CheckpointStore>,
                Arc<dyn object_store::ObjectStore>,
            ) = if let Some(ref url) = self.config.object_store_url {
                let obj = laminar_core::storage::object_store_builder::build_object_store(
                    url,
                    &self.config.object_store_options,
                )
                .map_err(|e| DbError::Config(format!("object store: {e}")))?;
                let cs = laminar_core::storage::checkpoint_store::ObjectStoreCheckpointStore::new(
                    Arc::clone(&obj),
                    String::new(),
                    max_retained,
                )
                .with_vnode_count(vnode_count);
                (Box::new(cs), obj)
            } else {
                std::fs::create_dir_all(&data_dir).map_err(|e| {
                    DbError::Config(format!("data dir {}: {e}", data_dir.display()))
                })?;
                let obj: Arc<dyn object_store::ObjectStore> = Arc::new(
                    object_store::local::LocalFileSystem::new_with_prefix(&data_dir)
                        .map_err(|e| DbError::Config(format!("local fs: {e}")))?,
                );
                let cs = laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(
                    &data_dir,
                    max_retained,
                )
                .with_vnode_count(vnode_count);
                (Box::new(cs), obj)
            };

            let defaults = CkpConfig::default();
            let config = CkpConfig {
                interval: cp_config.interval_ms.map(std::time::Duration::from_millis),
                max_retained,
                max_in_flight_epochs: cp_config
                    .max_in_flight_epochs
                    .unwrap_or(defaults.max_in_flight_epochs),
                max_staged_bytes: cp_config
                    .max_staged_bytes
                    .unwrap_or(defaults.max_staged_bytes),
                ..defaults
            };
            let mut coord = CheckpointCoordinator::new(config, store).await?;
            if let Some(ref prom) = *self.engine_metrics.lock() {
                coord.set_metrics(Arc::clone(prom));
            }

            #[cfg(feature = "cluster")]
            if let Some(controller) = self.cluster_controller.lock().clone() {
                coord.set_cluster_controller(controller);
            }
            #[cfg(feature = "cluster")]
            if let Some(watch) = self.leader_lease_watch.lock().clone() {
                coord.set_leader_lease_watch(watch);
            }

            if let (Some(backend), Some(registry)) = (
                self.state_backend.lock().clone(),
                self.vnode_registry.lock().clone(),
            ) {
                let owner = {
                    #[cfg(feature = "cluster")]
                    {
                        self.cluster_controller
                            .lock()
                            .as_ref()
                            .map_or(laminar_core::state::NodeId(0), |c| {
                                laminar_core::state::NodeId(c.instance_id().0)
                            })
                    }
                    #[cfg(not(feature = "cluster"))]
                    {
                        laminar_core::state::NodeId(0)
                    }
                };
                // Coordinator and backend must agree on the same generation —
                // the coordinator stamps writes, the backend rejects stale ones.
                // Without the backend call the fence is a silent no-op.
                let version = registry.assignment_version();
                backend.set_authoritative_version(version);
                coord.set_state_backend(backend);
                coord.set_assignment_version(version);
                coord.set_vnode_set(laminar_core::state::owned_vnodes(&registry, owner));
                // Leader gate covers all instances; 2PC only fires when every follower committed.
                coord.set_gate_vnode_set((0..registry.vnode_count()).collect());
            }

            let ds = {
                #[cfg(feature = "cluster")]
                {
                    self.decision_store.lock().clone().unwrap_or_else(|| {
                        Arc::new(
                            laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
                                Arc::clone(&decision_backing),
                            ),
                        )
                    })
                }
                #[cfg(not(feature = "cluster"))]
                {
                    Arc::new(
                        laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
                            Arc::clone(&decision_backing),
                        ),
                    )
                }
            };
            coord.set_decision_store(ds);

            coord.reconcile_prepared_on_init().await;

            *self.coordinator.lock().await = Some(coord);
        }

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
            )
            .await?;
        } else {
            tracing::info!(
                sources = source_regs.len(),
                sinks = sink_regs.len(),
                "Starting in embedded (in-memory) mode — no streams"
            );
        }

        Ok(())
    }

    /// Build and start the unified pipeline with sources, sinks, and streams.
    #[allow(clippy::too_many_lines)]
    async fn start_connector_pipeline(
        &self,
        source_regs: HashMap<String, crate::connector_manager::SourceRegistration>,
        sink_regs: HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_regs: HashMap<String, crate::connector_manager::StreamRegistration>,
        table_regs: HashMap<String, crate::connector_manager::TableRegistration>,
        has_external: bool,
    ) -> Result<(), DbError> {
        use crate::connector_manager::{
            build_sink_config, build_source_config, build_table_config,
        };
        use crate::operator_graph::OperatorGraph;
        use crate::pipeline::{PipelineConfig, SourceRegistration};
        use laminar_connectors::reference::{ReferenceTableSource, RefreshMode};

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
        for (name, schema) in lookup_tables {
            let provider = crate::table_provider::ReferenceTableProvider::new(
                name.clone(),
                schema,
                self.table_store.clone(),
            );
            if let Err(e) = ctx.register_table(&name, Arc::new(provider)) {
                tracing::warn!(
                    table = %name,
                    error = %e,
                    "failed to register lookup table in operator graph context"
                );
            }
        }

        let mut graph = OperatorGraph::new(ctx);
        graph.set_max_state_bytes(self.config.max_state_bytes_per_operator);
        graph.set_lookup_registry(Arc::clone(&self.lookup_registry));
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
                graph.set_rehydration_handle(Arc::clone(&self.rehydrated_vnode_state));
            }
        }

        for name in source_regs.keys() {
            if let Some(entry) = self.catalog.get_source(name) {
                graph.register_source_schema(name.clone(), entry.schema.clone());
            }
        }

        let partial_lookup_tables: rustc_hash::FxHashMap<String, Vec<String>> = table_regs
            .values()
            .filter(|r| matches!(r.refresh, Some(RefreshMode::Manual)))
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

        // `set_state_tier` must precede `add_query` so operators built below pick up the sender.
        #[cfg(feature = "state-tier")]
        let state_tier_sender: Option<crate::state_tier::TierTx> = {
            match self.config.state_tier_dir.clone() {
                Some(dir) => {
                    let has_backend = self.state_backend.lock().is_some();
                    if graph.vnode_count().is_none() {
                        if let Some(registry) = self.vnode_registry.lock().clone() {
                            // Single-node (no controller): take vnode count from the registry.
                            graph.set_vnode_count(registry.vnode_count());
                        }
                    }
                    let has_topology = graph.vnode_count().is_some();
                    if has_backend && has_topology {
                        let handle = self
                            .ai_handle
                            .clone()
                            .unwrap_or_else(tokio::runtime::Handle::current);
                        let metrics = self.engine_metrics.lock().clone();
                        match crate::state_tier::StateTierStore::open(&dir, metrics) {
                            Ok(store) => {
                                let sender =
                                    crate::state_tier::spawn_worker(&handle, Arc::new(store), 256);
                                graph.set_state_tier(sender.clone());
                                tracing::info!(dir = %dir.display(), "state cold tier enabled");
                                Some(sender)
                            }
                            Err(e) => {
                                tracing::error!(error = %e, dir = %dir.display(), "failed to open state cold tier — demotion disabled");
                                None
                            }
                        }
                    } else {
                        tracing::warn!(
                            has_backend,
                            has_topology,
                            "state_tier_dir set but demotion disabled — the tier \
                             needs a durable [state] backend (holds demoted state \
                             for restart) and a vnode registry (single-node: a \
                             single-owner registry)"
                        );
                        None
                    }
                }
                None => None,
            }
        };
        #[cfg(feature = "state-tier")]
        if let Some(ref sender) = state_tier_sender {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                coord.set_state_tier(sender.clone());
            }
        }

        for reg in stream_regs.values() {
            graph.add_query(
                reg.name.clone(),
                reg.query_sql.clone(),
                reg.emit_clause.clone(),
                reg.window_config.clone(),
                reg.order_config.clone(),
                None,
                reg.join_config.clone(),
            );
        }
        graph.take_build_errors()?;

        for tcfg in graph.temporal_join_configs() {
            if self.lookup_registry.get_entry(&tcfg.table_name).is_none() {
                let initial_batch = self
                    .table_store
                    .read()
                    .to_record_batch(&tcfg.table_name)
                    .or_else(|| {
                        self.catalog
                            .get_source(&tcfg.table_name)
                            .map(|e| RecordBatch::new_empty(e.schema.clone()))
                    })
                    .unwrap_or_else(|| {
                        RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()))
                    });
                let key_columns = vec![tcfg.table_key_column.clone()];
                let key_indices: Vec<usize> = key_columns
                    .iter()
                    .filter_map(|k| initial_batch.schema().index_of(k).ok())
                    .collect();

                // If the AS OF clause didn't resolve a version column, pick the first
                // timestamp/int column that isn't the join key.
                let resolved_version_col = if tcfg.table_version_column.is_empty() {
                    let schema = initial_batch.schema();
                    schema
                        .fields()
                        .iter()
                        .find(|f| {
                            f.name() != &tcfg.table_key_column
                                && matches!(
                                    f.data_type(),
                                    arrow::datatypes::DataType::Int64
                                        | arrow::datatypes::DataType::Timestamp(_, _)
                                )
                        })
                        .map(|f| f.name().clone())
                        .unwrap_or_default()
                } else {
                    tcfg.table_version_column.clone()
                };

                let Ok(version_col_idx) = initial_batch.schema().index_of(&resolved_version_col)
                else {
                    if !initial_batch.schema().fields().is_empty() {
                        tracing::warn!(
                            table=%tcfg.table_name,
                            version_col=%resolved_version_col,
                            "Version column not found in temporal table schema; \
                             will resolve on first CDC batch"
                        );
                    }
                    // Index built on first CDC update.
                    self.lookup_registry.register_versioned(
                        &tcfg.table_name,
                        laminar_sql::datafusion::VersionedLookupState {
                            batch: initial_batch,
                            index: Arc::new(
                                laminar_sql::datafusion::lookup_join_exec::VersionedIndex::default(
                                ),
                            ),
                            key_columns,
                            version_column: resolved_version_col,
                            stream_time_column: tcfg.stream_time_column.clone(),
                            max_versions_per_key: usize::MAX,
                        },
                    );
                    continue;
                };
                let index = Arc::new(
                    laminar_sql::datafusion::lookup_join_exec::VersionedIndex::build(
                        &initial_batch,
                        &key_indices,
                        version_col_idx,
                        usize::MAX,
                    )
                    .unwrap_or_default(),
                );
                self.lookup_registry.register_versioned(
                    &tcfg.table_name,
                    laminar_sql::datafusion::VersionedLookupState {
                        batch: initial_batch,
                        index,
                        key_columns,
                        version_column: resolved_version_col,
                        stream_time_column: tcfg.stream_time_column.clone(),
                        max_versions_per_key: usize::MAX,
                    },
                );
            }
        }

        let prom_registry = self.prometheus_registry.lock().clone();

        let mut sources: Vec<SourceRegistration> = Vec::new();
        for (name, reg) in &source_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let mut config = build_source_config(reg)?;

            if let Some(entry) = self.catalog.get_source(name) {
                let schema_str = crate::pipeline_callback::encode_arrow_schema(&entry.schema);
                config.set("_arrow_schema".to_string(), schema_str);
            }

            #[cfg_attr(not(feature = "cluster"), allow(unused_mut))]
            let mut source = self
                .connector_registry
                .create_source(&config, prom_registry.as_deref())
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Cannot create source '{}' (type '{}'): {e}",
                        name,
                        config.connector_type()
                    ))
                })?;
            #[cfg(feature = "cluster")]
            if let (Some(registry), Some(self_id)) = (
                self.vnode_registry.lock().clone(),
                self.cluster_controller
                    .lock()
                    .as_ref()
                    .map(|c| laminar_core::state::NodeId(c.instance_id().0)),
            ) {
                source.set_vnode_assignment(registry, self_id);
            }
            let supports_replay = source.supports_replay();
            if !supports_replay {
                tracing::warn!(
                    source = %name,
                    "source does not support replay — exactly-once semantics \
                     are degraded to at-most-once for this source"
                );
            }
            // Connector-property watermark wiring: `event.time.column` (Kafka)
            // and `event.time.field` (WebSocket) as an alternative to `WATERMARK FOR`.
            if let Some(entry) = self.catalog.get_source(name) {
                if entry.source.event_time_column().is_none() {
                    if let Some(col) = config.get("event.time.column") {
                        entry.source.set_event_time_column(col);
                    } else if let Some(col) = config.get("event.time.field") {
                        entry.source.set_event_time_column(col);
                    }
                }
                if let Some(ms_str) = config.get("max.out.of.orderness.ms") {
                    match ms_str.parse::<u64>() {
                        Ok(ms) => {
                            entry
                                .source
                                .set_max_out_of_orderness(std::time::Duration::from_millis(ms));
                        }
                        Err(e) => {
                            tracing::warn!(
                                source = %name,
                                value = %ms_str,
                                error = %e,
                                "ignoring unparseable max.out.of.orderness.ms — \
                                 watermark will use Duration::ZERO"
                            );
                        }
                    }
                }
            }

            sources.push(SourceRegistration {
                name: name.clone(),
                connector: source,
                config,
                supports_replay,
                restore_checkpoint: None, // Set after recovery below
            });
        }

        let bridged_names: rustc_hash::FxHashSet<String> =
            sources.iter().map(|s| s.name.clone()).collect();
        for (name, reg) in &source_regs {
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
                sources.push(SourceRegistration {
                    name: name.clone(),
                    connector: Box::new(connector),
                    config: laminar_connectors::config::ConnectorConfig::new("catalog-bridge"),
                    supports_replay: false,
                    restore_checkpoint: None,
                });
            }
        }
        for name in self.catalog.list_sources() {
            if bridged_names.contains(&name) || source_regs.contains_key(&name) {
                continue;
            }
            if let Some(entry) = self.catalog.get_source(&name) {
                graph.register_source_schema(name.clone(), entry.schema.clone());
                let subscription = entry.sink.subscribe();
                let connector = crate::catalog_connector::CatalogSourceConnector::new(
                    subscription,
                    entry.schema.clone(),
                    entry.data_notify(),
                );
                sources.push(SourceRegistration {
                    name: name.clone(),
                    connector: Box::new(connector),
                    config: laminar_connectors::config::ConnectorConfig::new("catalog-bridge"),
                    supports_replay: false,
                    restore_checkpoint: None,
                });
            }
        }

        let stream_output_schemas = resolve_stream_output_schemas(&self.ctx, &stream_regs).await?;
        {
            let mut schemas = self.stream_schemas.write();
            schemas.clear();
            schemas.extend(
                stream_output_schemas
                    .iter()
                    .map(|(k, v)| (k.clone(), Arc::clone(v))),
            );
        }

        let (sink_event_tx, sink_event_rx) =
            laminar_core::streaming::channel::channel::<crate::sink_task::SinkEvent>(
                crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY,
            );
        #[allow(clippy::type_complexity)]
        let mut sinks: Vec<(
            String,
            crate::sink_task::SinkTaskHandle,
            Option<String>,
            String, // input stream name (FROM clause target)
            bool,   // changelog-capable
        )> = Vec::new();
        for (name, reg) in &sink_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let mut config = build_sink_config(reg)?;
            let upstream_schema = stream_output_schemas.get(&reg.input).cloned().or_else(|| {
                self.catalog
                    .get_source(&reg.input)
                    .map(|e| e.schema.clone())
            });
            if let Some(schema) = upstream_schema {
                let schema_str = crate::pipeline_callback::encode_arrow_schema(&schema);
                config.set("_arrow_schema".to_string(), schema_str);
            }
            let mut sink = self
                .connector_registry
                .create_sink(&config, prom_registry.as_deref())
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Cannot create sink '{}' (type '{}'): {e}",
                        name,
                        config.connector_type()
                    ))
                })?;
            sink.open(&config)
                .await
                .map_err(|e| DbError::Connector(format!("Failed to open sink '{name}': {e}")))?;
            let caps = sink.capabilities();
            let write_timeout =
                match config
                    .get_parsed::<u64>("sink.write.timeout.ms")
                    .map_err(|e| {
                        DbError::Connector(format!(
                            "Invalid 'sink.write.timeout.ms' for sink '{name}': {e}"
                        ))
                    })? {
                    Some(ms) => std::time::Duration::from_millis(ms),
                    None => caps.suggested_write_timeout,
                };
            if write_timeout.is_zero() {
                return Err(DbError::Connector(format!(
                    "sink '{name}': write_timeout must be > 0 \
                     (check 'sink.write.timeout.ms' or the sink's \
                     suggested_write_timeout)"
                )));
            }
            let sink_id: std::sync::Arc<str> = std::sync::Arc::from(name.as_str());
            let handle =
                crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
                    name: name.clone(),
                    sink_id,
                    connector: sink,
                    exactly_once: caps.exactly_once,
                    coordinated_commit: caps.coordinated_commit,
                    channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
                    flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
                    write_timeout,
                    event_tx: sink_event_tx.clone(),
                });
            sinks.push((
                name.clone(),
                handle,
                reg.filter_expr.clone(),
                reg.input.clone(),
                caps.changelog,
            ));
        }
        drop(sink_event_tx);

        let mut table_sources: Vec<(String, Box<dyn ReferenceTableSource>, RefreshMode)> =
            Vec::new();
        for (name, reg) in &table_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let config = build_table_config(reg)?;
            let source = self
                .connector_registry
                .create_table_source(&config)
                .map_err(|e| {
                    DbError::Connector(format!("Cannot create table source '{name}': {e}"))
                })?;
            let mode = reg.refresh.clone().unwrap_or(RefreshMode::SnapshotPlusCdc);
            table_sources.push((name.clone(), source, mode));
        }

        let (coordinated_committer, committer_poll) = {
            let mut guard = self.coordinator.lock().await;
            match *guard {
                Some(ref mut coord) => {
                    for (name, handle, _, _, _) in &sinks {
                        coord.register_sink(
                            name.clone(),
                            handle.clone(),
                            handle.exactly_once(),
                            handle.coordinated_commit(),
                        );
                    }
                    (
                        coord.coordinated_committer(),
                        coord.committer_poll_interval(),
                    )
                }
                None => (None, std::time::Duration::from_secs(1)),
            }
        };

        // Decoupled designated committer: a poll loop off the barrier path that
        // commits sealed epochs for coordinated-commit sinks. Spawned only when
        // such sinks exist; stops on terminal pipeline state. The handle is held
        // so shutdown can abort it (its work is idempotent).
        if let Some(mut committer) = coordinated_committer {
            let state = Arc::clone(&self.state);
            let handle = tokio::spawn(async move {
                let mut tick = tokio::time::interval(committer_poll);
                loop {
                    tick.tick().await;
                    if matches!(
                        DbState::load(&state),
                        DbState::Stopped | DbState::ShuttingDown
                    ) {
                        break;
                    }
                    if let Err(e) = committer.commit_ready().await {
                        tracing::warn!(error = %e, "coordinated committer pass failed; will retry");
                    }
                }
            });
            *self.committer_handle.lock() = Some(handle);
        }

        // Must run BEFORE begin_initial_epoch so the epoch reflects the recovered state.
        // Hoist watermarks now so generators are seeded before watermark-state construction;
        // without this, generators restart at i64::MIN while offsets resume mid-stream.
        let mut recovered_source_wms: rustc_hash::FxHashMap<String, i64> =
            rustc_hash::FxHashMap::default();
        {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                match coord.recover().await {
                    Ok(Some(recovered)) => {
                        recovered_source_wms = recovered
                            .manifest
                            .source_watermarks
                            .iter()
                            .filter(|(_, &wm)| wm != i64::MIN)
                            .map(|(name, &wm)| (name.clone(), wm))
                            .collect();
                        for (name, source, _) in &mut table_sources {
                            if let Some(cp) = recovered.manifest.table_offsets.get(name) {
                                let restored =
                                    crate::checkpoint_coordinator::connector_to_source_checkpoint(
                                        cp,
                                    );
                                if let Err(e) = source.restore(&restored).await {
                                    tracing::warn!(
                                        table=%name, error=%e,
                                        "Table source restore failed"
                                    );
                                }
                            }
                        }
                        for src in &mut sources {
                            if !src.supports_replay {
                                continue;
                            }
                            if let Some(cp) = recovered.manifest.source_offsets.get(&src.name) {
                                let restored =
                                    crate::checkpoint_coordinator::connector_to_source_checkpoint(
                                        cp,
                                    );
                                tracing::info!(
                                    source = %src.name,
                                    offsets = cp.offsets.len(),
                                    "attaching checkpoint offsets for source recovery"
                                );
                                src.restore_checkpoint = Some(restored);
                            }
                        }
                        let mut graph_restore_failed = false;
                        let op_keys: Vec<&String> =
                            recovered.manifest.operator_states.keys().collect();
                        let instance_hint = {
                            #[cfg(feature = "cluster")]
                            {
                                self.cluster_controller
                                    .lock()
                                    .as_ref()
                                    .map_or(0, |c| c.instance_id().0)
                            }
                            #[cfg(not(feature = "cluster"))]
                            {
                                0u64
                            }
                        };
                        tracing::info!(
                            instance = instance_hint,
                            count = op_keys.len(),
                            keys = ?op_keys,
                            "manifest operator_states summary"
                        );
                        if let Some(op) = recovered.manifest.operator_states.get("operator_graph") {
                            if let Some(bytes) = op.decode_inline() {
                                match graph.restore_from_bytes(&bytes) {
                                    Ok(n) => {
                                        tracing::info!(
                                            queries = n,
                                            "Restored operator graph state from checkpoint"
                                        );
                                    }
                                    Err(e) => {
                                        // Source offsets are already staged; resuming with
                                        // empty operator state would silently lose in-flight
                                        // windows. Fail loud so the intent to start fresh
                                        // must be explicit.
                                        return Err(DbError::Checkpoint(format!(
                                            "[LDB-6029] operator graph restore failed: \
                                             {e} — refusing to start with checkpointed \
                                             source offsets and empty operator state"
                                        )));
                                    }
                                }
                            } else {
                                tracing::warn!(
                                    "manifest has 'operator_graph' but decode_inline returned None"
                                );
                            }
                        } else if recovered
                            .manifest
                            .operator_states
                            .contains_key("stream_executor")
                        {
                            graph_restore_failed = true;
                            tracing::warn!(
                                "Found old stream_executor checkpoint format; \
                                 skipping restore (clean break). Starting fresh."
                            );
                        }

                        // Cold-tier vnodes are wiped on restart; replay them from durable partials.
                        // Only the demoting operator's slice is applied — others restored from
                        // the manifest (double-applying would corrupt state).
                        #[cfg(feature = "state-tier")]
                        if !graph_restore_failed {
                            let cold_map = graph.take_tier_cold_vnodes();
                            if !cold_map.is_empty() {
                                self.rehydrate_cold_vnodes(&mut graph, &cold_map).await?;
                            }
                        }

                        if !graph_restore_failed {
                            let prefix = crate::mv_store::CHECKPOINT_KEY_PREFIX;
                            let mut store = self.mv_store.write();
                            let mut restored = 0usize;
                            for (key, op) in &recovered.manifest.operator_states {
                                if let Some(name) = key.strip_prefix(prefix) {
                                    if let Some(bytes) = op.decode_inline() {
                                        match store.restore_from_ipc(name, &bytes) {
                                            Ok(true) => restored += 1,
                                            Ok(false) => {}
                                            Err(e) => {
                                                tracing::warn!(mv = name, error = %e, "MV restore failed");
                                            }
                                        }
                                    }
                                }
                            }
                            if restored > 0 {
                                tracing::info!(mvs = restored, "Restored MV state from checkpoint");
                            }
                        }
                        tracing::info!(
                            epoch = recovered.epoch(),
                            sources_restored = recovered.sources_restored,
                            sinks_rolled_back = recovered.sinks_rolled_back,
                            "Recovered from unified checkpoint"
                        );
                    }
                    Ok(None) => {
                        tracing::info!("No checkpoint found, starting fresh");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Checkpoint recovery failed, starting fresh");
                    }
                }
            }
        }

        {
            let guard = self.coordinator.lock().await;
            if let Some(ref coord) = *guard {
                coord.begin_initial_epoch().await?;
            }
        }

        for (name, source, mode) in &mut table_sources {
            if matches!(mode, RefreshMode::Manual) {
                continue;
            }
            while let Some(batch) = source
                .poll_snapshot()
                .await
                .map_err(|e| DbError::Connector(format!("Table '{name}' snapshot error: {e}")))?
            {
                self.table_store
                    .write()
                    .upsert(name, &batch)
                    .map_err(|e| DbError::Connector(format!("Table '{name}' upsert error: {e}")))?;
            }
            self.sync_table_to_datafusion(name)?;
            {
                let mut ts = self.table_store.write();
                ts.rebuild_xor_filter(name);
                ts.set_ready(name, true);
            }
            // Setup built the Versioned (temporal-join) state before the
            // snapshot existed; rebuild it over the snapshot now instead of
            // downgrading it to a plain Snapshot.
            let entry = self.lookup_registry.get_entry(name);
            if let Some(laminar_sql::datafusion::RegisteredLookup::Versioned(v)) = &entry {
                if let Some(batch) = self.table_store.read().to_record_batch(name) {
                    if let Some(state) = crate::pipeline_callback::rebuild_versioned_state(v, batch)
                    {
                        self.lookup_registry.register_versioned(name, state);
                    }
                }
            } else if let Some(batch) = self.table_store.read().to_record_batch(name) {
                self.lookup_registry
                    .register(name, laminar_sql::datafusion::LookupSnapshot { batch });
            }
        }

        for (name, _source, mode) in &mut table_sources {
            if !matches!(mode, RefreshMode::Manual) {
                continue;
            }
            let Some(reg) = table_regs.get(name.as_str()) else {
                continue;
            };
            let capacity_bytes = reg.cache_max_bytes.unwrap_or(64 * 1024 * 1024);
            let Some(schema) = self.table_store.read().table_schema(name) else {
                continue;
            };
            let pk_csv = &reg.primary_key;
            let pk_cols: Vec<String> = pk_csv
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let key_sort_fields: Vec<arrow::row::SortField> = pk_cols
                .iter()
                .filter_map(|col| {
                    schema
                        .field_with_name(col)
                        .ok()
                        .map(|f| arrow::row::SortField::new(f.data_type().clone()))
                })
                .collect();

            let cache = Arc::new(laminar_core::lookup::lookup_cache::LookupMemoryCache::new(
                0,
                laminar_core::lookup::lookup_cache::LookupMemoryCacheConfig {
                    capacity_bytes,
                    ttl: reg.cache_ttl,
                },
            ));
            let lookup_source = if let Ok(mut config) = build_table_config(reg) {
                config.set("_primary_key_columns", pk_csv.as_str());
                match self
                    .connector_registry
                    .create_lookup_source(config, Some(Arc::clone(&schema)))
                    .await
                {
                    Some(Ok(src)) => Some(src),
                    Some(Err(e)) => {
                        tracing::warn!(
                            table = %name, error = %e,
                            "lookup source creation failed; cache-only mode"
                        );
                        None
                    }
                    None => None,
                }
            } else {
                None
            };

            let projection = crate::sql_analysis::compute_lookup_projection(
                &schema,
                &pk_cols,
                name.as_str(),
                stream_regs.values().map(|r| r.query_sql.as_str()),
            );

            self.lookup_registry.register_partial(
                name,
                laminar_sql::datafusion::PartialLookupState {
                    lookup_cache: cache,
                    schema,
                    key_columns: pk_cols,
                    key_sort_fields,
                    source: lookup_source,
                    fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(16)),
                    projection,
                },
            );
            *mode = RefreshMode::SnapshotPlusCdc;
            tracing::info!(
                table = %name,
                capacity_bytes,
                ttl = ?reg.cache_ttl,
                pk = %pk_csv,
                "registered on-demand lookup table (partial cache)"
            );
        }

        let mut stream_sources: Vec<(String, streaming::Source<crate::catalog::ArrowRecord>)> =
            Vec::new();
        for reg in stream_regs.values() {
            if let Some(src) = self.catalog.get_stream_source(&reg.name) {
                stream_sources.push((reg.name.clone(), src));
            }
        }

        // `LAMINAR_MAX_FUTURE_SKEW_MS=0` disables the future-skew ceiling (legacy unbounded).
        let future_skew_ms = match std::env::var("LAMINAR_MAX_FUTURE_SKEW_MS") {
            Ok(v) => v.parse::<i64>().unwrap_or_else(|_| {
                tracing::warn!(
                    value = %v,
                    "invalid LAMINAR_MAX_FUTURE_SKEW_MS (expected an integer); \
                     using the default"
                );
                laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS
            }),
            Err(_) => laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS,
        };
        let source_names = self.catalog.list_sources();
        let mut watermark_states: FxHashMap<String, SourceWatermarkState> =
            FxHashMap::with_capacity_and_hasher(source_names.len(), rustc_hash::FxBuildHasher);
        let mut source_entries_for_wm: FxHashMap<String, Arc<crate::catalog::SourceEntry>> =
            FxHashMap::with_capacity_and_hasher(source_names.len(), rustc_hash::FxBuildHasher);
        let mut source_ids: FxHashMap<String, usize> =
            FxHashMap::with_capacity_and_hasher(source_names.len(), rustc_hash::FxBuildHasher);
        for name in source_names {
            if let Some(entry) = self.catalog.get_source(&name) {
                if let (Some(col), Some(dur)) =
                    (&entry.watermark_column, entry.max_out_of_orderness)
                {
                    let extractor = laminar_core::time::EventTimeExtractor::from_column(col)
                        .with_mode(laminar_core::time::ExtractionMode::Max);
                    let generator: Box<dyn laminar_core::time::WatermarkGenerator> = if entry
                        .is_processing_time
                        .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        Box::new(laminar_core::time::ProcessingTimeGenerator::new())
                    } else {
                        Box::new(
                            laminar_core::time::BoundedOutOfOrdernessGenerator::from_duration(dur)
                                .with_max_future_skew(future_skew_ms),
                        )
                    };
                    let id = source_ids.len();
                    source_ids.insert(name.clone(), id);
                    watermark_states.insert(
                        name.clone(),
                        SourceWatermarkState {
                            extractor,
                            generator,
                            column: col.clone(),
                        },
                    );
                }
                source_entries_for_wm.insert(name, entry);
            }
        }

        // Fallback watermark path for sources that use the programmatic API
        // or connector properties instead of `WATERMARK FOR`.
        for name in self.catalog.list_sources() {
            if watermark_states.contains_key(&name) {
                continue;
            }
            if let Some(entry) = self.catalog.get_source(&name) {
                if let Some(col) = entry.source.event_time_column() {
                    let extractor = laminar_core::time::EventTimeExtractor::from_column(&col)
                        .with_mode(laminar_core::time::ExtractionMode::Max);
                    let ooo_bound = entry
                        .source
                        .max_out_of_orderness()
                        .unwrap_or(std::time::Duration::ZERO);
                    let generator: Box<dyn laminar_core::time::WatermarkGenerator> = if entry
                        .is_processing_time
                        .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        Box::new(laminar_core::time::ProcessingTimeGenerator::new())
                    } else {
                        Box::new(
                            laminar_core::time::BoundedOutOfOrdernessGenerator::from_duration(
                                ooo_bound,
                            )
                            .with_max_future_skew(future_skew_ms),
                        )
                    };
                    let id = source_ids.len();
                    source_ids.insert(name.clone(), id);
                    watermark_states.insert(
                        name.clone(),
                        SourceWatermarkState {
                            extractor,
                            generator,
                            column: col,
                        },
                    );
                }
            }
        }

        // LAMINAR_SOURCE_IDLE_TIMEOUT_MS > 0 enables idle-source detection; unset/0 = disabled.
        let idle_timeout_ms: Option<u64> = match std::env::var("LAMINAR_SOURCE_IDLE_TIMEOUT_MS") {
            Ok(v) => match v.parse::<u64>() {
                Ok(0) => None,
                Ok(ms) => Some(ms),
                Err(_) => {
                    tracing::warn!(
                        value = %v,
                        "invalid LAMINAR_SOURCE_IDLE_TIMEOUT_MS (expected a non-negative \
                         integer); idle-source detection disabled"
                    );
                    None
                }
            },
            Err(_) => None,
        };
        let mut tracker = if source_ids.is_empty() {
            None
        } else {
            let mut t = laminar_core::time::WatermarkTracker::new(source_ids.len());
            if let Some(ms) = idle_timeout_ms {
                let d = std::time::Duration::from_millis(ms);
                for id in 0..source_ids.len() {
                    t.set_idle_timeout(id, Some(d));
                }
            }
            Some(t)
        };

        // Mixed watermarked/un-watermarked sources: un-watermarked ones inherit
        // the global clock, so joins/windows close on the watermarked source's
        // time. Surface the mismatch rather than silently dropping late rows.
        let registered = self.catalog.list_sources();
        let unwatermarked: Vec<&str> = registered
            .iter()
            .filter(|n| !source_ids.contains_key(*n))
            .map(String::as_str)
            .collect();
        if !source_ids.is_empty() && !unwatermarked.is_empty() {
            tracing::warn!(
                watermarked = source_ids.len(),
                unwatermarked = unwatermarked.len(),
                unwatermarked_names = ?unwatermarked,
                "Pipeline mixes watermarked and un-watermarked sources. An un-watermarked \
                 source in a join/window inherits the global watermark — time-based \
                 operators may behave unexpectedly. Add `WATERMARK FOR` to the missing \
                 sources or split into separate pipelines."
            );
        }

        if !recovered_source_wms.is_empty() {
            let mut combined = i64::MIN;
            for (name, wm) in &recovered_source_wms {
                if let Some(state) = watermark_states.get_mut(name) {
                    let _ = state.generator.advance_watermark(*wm);
                }
                if let (Some(t), Some(&id)) = (tracker.as_mut(), source_ids.get(name)) {
                    if let Some(global) = t.update_source(id, *wm) {
                        combined = combined.max(global.timestamp());
                    }
                }
            }
            if combined != i64::MIN {
                self.pipeline_watermark
                    .store(combined, std::sync::atomic::Ordering::SeqCst);
                tracing::info!(
                    sources = recovered_source_wms.len(),
                    pipeline_watermark = combined,
                    "Restored watermarks from checkpoint"
                );
            }
        }

        let max_poll = self.config.default_buffer_size.min(1024);
        let checkpoint_interval = self
            .config
            .checkpoint
            .as_ref()
            .and_then(|c| c.interval_ms)
            .map(std::time::Duration::from_millis);

        tracing::info!(
            sources = sources.len(),
            sinks = sinks.len(),
            streams = stream_regs.len(),
            subscriptions = stream_sources.len(),
            watermark_sources = source_ids.len(),
            "Starting event-driven connector pipeline"
        );

        let drain_budget_ns = self.config.pipeline_drain_budget_ns.unwrap_or(1_000_000);
        let query_budget_ns = self.config.pipeline_query_budget_ns.unwrap_or(8_000_000);
        let pipeline_config = PipelineConfig {
            max_poll_records: max_poll,
            channel_capacity: self.config.pipeline_channel_capacity.unwrap_or(64),
            fallback_poll_interval: if has_external {
                std::time::Duration::from_millis(10)
            } else {
                std::time::Duration::from_millis(1)
            },
            checkpoint_interval,
            batch_window: self
                .config
                .pipeline_batch_window
                .unwrap_or(if has_external {
                    std::time::Duration::from_millis(5)
                } else {
                    std::time::Duration::ZERO
                }),
            barrier_alignment_timeout: self
                .config
                .checkpoint
                .as_ref()
                .and_then(|c| c.alignment_timeout_ms)
                .map_or(
                    std::time::Duration::from_secs(30),
                    std::time::Duration::from_millis,
                ),
            delivery_guarantee: self.config.delivery_guarantee,
            cycle_budget_ns: 10_000_000_u64.max(drain_budget_ns + query_budget_ns),
            drain_budget_ns,
            query_budget_ns,
            background_budget_ns: 5_000_000, // 5ms
            max_input_buf_batches: self.config.pipeline_max_input_buf_batches.unwrap_or(256),
            max_input_buf_bytes: self.config.pipeline_max_input_buf_bytes,
            backpressure_policy: self.config.pipeline_backpressure_policy,
        };

        {
            use laminar_connectors::connector::DeliveryGuarantee;

            if pipeline_config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
                for src in &sources {
                    if !src.supports_replay {
                        return Err(DbError::Config(format!(
                            "[LDB-5030] exactly-once requires all sources to support replay, \
                             but source '{}' does not. Use at-least-once or remove this source.",
                            src.name
                        )));
                    }
                }
                for (name, handle, _, _, _) in &sinks {
                    if !handle.exactly_once() {
                        return Err(DbError::Config(format!(
                            "[LDB-5031] exactly-once requires all sinks to support \
                             exactly-once semantics, but sink '{name}' does not. \
                             Use at-least-once or configure a transactional sink."
                        )));
                    }
                }
                if pipeline_config.checkpoint_interval.is_none() {
                    return Err(DbError::Config(
                        "[LDB-5032] exactly-once requires checkpointing to be enabled. \
                         Set checkpoint.interval.ms in the pipeline configuration."
                            .into(),
                    ));
                }
            } else if pipeline_config.delivery_guarantee == DeliveryGuarantee::AtLeastOnce {
                let has_non_replayable = sources.iter().any(|s| !s.supports_replay);
                let has_eo_sink = sinks.iter().any(|(_, h, _, _, _)| h.exactly_once());
                if has_non_replayable && has_eo_sink {
                    tracing::warn!(
                        "[LDB-5033] pipeline has exactly-once sinks but some sources \
                         do not support replay — effective guarantee is at-most-once \
                         for events from non-replayable sources"
                    );
                }
            }
        }

        let shutdown = self.shutdown_signal.clone();

        let pipeline_watermark = Arc::clone(&self.pipeline_watermark);
        let coordinator = Arc::clone(&self.coordinator);
        let table_store_for_loop = self.table_store.clone();
        let pipeline_hash = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            for reg in stream_regs.values() {
                reg.name.hash(&mut hasher);
                reg.query_sql.hash(&mut hasher);
            }
            for name in source_regs.keys() {
                name.hash(&mut hasher);
            }
            for name in sink_regs.keys() {
                name.hash(&mut hasher);
            }
            Some(hasher.finish())
        };

        graph.set_query_budget_ns(pipeline_config.query_budget_ns);
        graph.set_max_input_buf_batches(pipeline_config.max_input_buf_batches);
        graph.set_max_input_buf_bytes(pipeline_config.max_input_buf_bytes);
        graph.set_backpressure_policy(pipeline_config.backpressure_policy);

        let sinks_pending_filter_count = sinks
            .iter()
            .filter(|(_, _, filter_sql, _, _)| filter_sql.is_some())
            .count();

        let source_name_arcs: rustc_hash::FxHashMap<usize, Arc<str>> = source_ids
            .iter()
            .map(|(name, &sid)| (sid, Arc::<str>::from(name.as_str())))
            .collect();
        let source_wms_buf = rustc_hash::FxHashMap::with_capacity_and_hasher(
            source_name_arcs.len(),
            rustc_hash::FxBuildHasher,
        );

        let prom = self
            .engine_metrics
            .lock()
            .clone()
            .expect("EngineMetrics must be set before start()");

        let (force_ckpt_tx, force_ckpt_rx) = crossfire::mpsc::bounded_async::<
            crate::db::ForceCheckpointReply,
        >(crate::db::FORCE_CHECKPOINT_CHANNEL_CAPACITY);
        *self.force_ckpt_tx.lock() = Some(force_ckpt_tx);

        let (checkpoint_complete_tx, checkpoint_complete_rx) = crossfire::mpsc::bounded_async::<(
            u64,
            rustc_hash::FxHashMap<String, laminar_connectors::checkpoint::SourceCheckpoint>,
        )>(16);
        // Cap in-flight epochs at 1 when any sink is exactly-once — DDL-configured
        // sinks declare this via capabilities, not the pipeline-level guarantee,
        // so checking capabilities guards against pipelining over an open transaction.
        let has_exactly_once_sink = sinks
            .iter()
            .any(|(_, handle, _, _, _)| handle.exactly_once());
        let checkpoint_in_flight = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let staged_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (epoch_allocator, ckpt_quorum_timeout, max_in_flight_epochs, max_staged_bytes) = {
            let guard = coordinator.lock().await;
            match guard.as_ref() {
                Some(coord) => {
                    let cfg = coord.config();
                    let depth = if has_exactly_once_sink
                        || pipeline_config.delivery_guarantee
                            == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce
                    {
                        1
                    } else {
                        cfg.max_in_flight_epochs.max(1)
                    };
                    (
                        Some(coord.epoch_allocator()),
                        cfg.quorum_timeout,
                        depth,
                        cfg.max_staged_bytes.max(1), // 0 would pause admission permanently
                    )
                }
                None => (None, std::time::Duration::from_secs(3), 1, u64::MAX),
            }
        };
        #[cfg(not(feature = "cluster"))]
        let _ = ckpt_quorum_timeout;

        let static_stream_names: rustc_hash::FxHashSet<Arc<str>> = stream_sources
            .iter()
            .map(|(name, _)| Arc::from(name.as_str()))
            .collect();

        let callback = crate::pipeline_callback::ConnectorPipelineCallback {
            graph,
            stream_sources,
            sinks,
            watermark_states,
            source_entries_for_wm,
            source_ids,
            source_name_arcs,
            source_wms_buf,
            tracker,
            prom,
            pipeline_watermark,
            coordinator,
            table_sources,
            table_store: table_store_for_loop,
            mv_store_has_any: self.mv_store.read().has_any_handle(),
            mv_store: self.mv_store.clone(),
            lookup_registry: Arc::clone(&self.lookup_registry),
            filter_ctx: laminar_sql::create_session_context(),
            compiled_sink_filters: Vec::new(),
            pending_sink_filter_compiles: sinks_pending_filter_count,
            last_checkpoint: std::time::Instant::now(),
            checkpoint_interval: self
                .config
                .checkpoint
                .as_ref()
                .and_then(|c| c.interval_ms)
                .map(std::time::Duration::from_millis),
            pipeline_hash,
            delivery_guarantee: pipeline_config.delivery_guarantee,
            serialization_timeout: std::time::Duration::from_secs(120),
            sink_event_rx,
            sink_timed_out: false,
            shutdown_signal: Arc::clone(&self.shutdown_signal),
            #[cfg(feature = "cluster")]
            cluster_controller: self.cluster_controller.lock().clone(),
            #[cfg(feature = "cluster")]
            follower_tail: Arc::default(),
            #[cfg(feature = "cluster")]
            barrier_injectors: Vec::new(),
            #[cfg(feature = "cluster")]
            pending_follower_checkpoint: None,
            force_ckpt_rx: Some(force_ckpt_rx),
            subscription_registry: Arc::clone(&self.subscription_registry),
            #[cfg(feature = "cluster")]
            active_subs: Arc::clone(&self.active_subs),
            #[cfg(feature = "cluster")]
            sub_route: std::sync::OnceLock::new(),
            static_stream_names,
            checkpoint_complete_tx,
            checkpoint_in_flight: Arc::clone(&checkpoint_in_flight),
            staged_bytes: Arc::clone(&staged_bytes),
            epoch_allocator,
            #[cfg(feature = "cluster")]
            quorum_timeout: ckpt_quorum_timeout,
            exactly_once_sinks: has_exactly_once_sink,
            state_memory_budget_bytes: self.config.state_memory_budget_bytes,
            // Backdated so the first cycle probes immediately.
            state_budget_probe_at: std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(3600))
                .unwrap_or_else(std::time::Instant::now),
            state_budget_exceeded: false,
            #[cfg(feature = "state-tier")]
            state_tier: state_tier_sender,
        };

        {
            let (control_tx, control_rx) =
                crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
            *self.control_tx.lock() = Some(control_tx);

            let coordinator = crate::pipeline::StreamingCoordinator::new(
                sources,
                pipeline_config,
                Arc::clone(&shutdown),
                control_rx,
            )
            .await?
            .with_checkpoint_complete_rx(checkpoint_complete_rx)
            .with_checkpoint_admission(
                checkpoint_in_flight,
                max_in_flight_epochs,
                staged_bytes,
                max_staged_bytes,
            );

            let (done_tx, done_rx) = crossfire::oneshot::oneshot::<()>();
            let (startup_tx, startup_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
            // Captured by the compute thread so an operator panic is recorded
            // (surfaced via pipeline status) rather than only logged.
            let fault_slot = Arc::clone(&self.last_fault);
            let fault_metrics = self.engine_metrics.lock().clone();
            match std::thread::Builder::new()
                .name("laminar-compute".into())
                .spawn(move || {
                    let rt = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => {
                            startup_tx.send(Ok(()));
                            rt
                        }
                        Err(e) => {
                            startup_tx.send(Err(format!("compute runtime: {e}")));
                            return;
                        }
                    };
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        rt.block_on(async move {
                            coordinator.run(callback).await;
                        });
                    }));
                    if let Err(panic) = result {
                        let msg = panic
                            .downcast_ref::<String>()
                            .map(String::as_str)
                            .or_else(|| panic.downcast_ref::<&str>().copied())
                            .unwrap_or("unknown");
                        tracing::error!(panic = msg, "laminar-compute thread panicked");
                        // Record before dropping done_tx so the watcher sees it.
                        *fault_slot.lock() = Some(msg.to_string());
                        if let Some(ref m) = fault_metrics {
                            m.pipeline_faults_total.inc();
                        }
                        return;
                    }
                    done_tx.send(());
                }) {
                Ok(_) => {}
                Err(e) => {
                    return Err(DbError::Config(format!(
                        "failed to spawn compute thread: {e}"
                    )));
                }
            }

            match startup_rx.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(DbError::Config(e)),
                Err(_) => {
                    return Err(DbError::Config(
                        "compute thread exited before starting runtime".into(),
                    ));
                }
            }

            let watcher_state = Arc::clone(&self.state);
            let watcher_shutdown = Arc::clone(&self.shutdown_signal);
            let watcher_fault = Arc::clone(&self.last_fault);
            let handle = tokio::spawn(async move {
                if done_rx.await.is_err() {
                    tracing::error!("laminar-compute thread exited unexpectedly");
                    watcher_fault
                        .lock()
                        .get_or_insert_with(|| "compute thread exited unexpectedly".to_string());
                    // Faulted, not Stopped — recoverable via a later start().
                    DbState::Faulted.store(&watcher_state);
                    watcher_shutdown.notify_one();
                }
            });

            *self.runtime_handle.lock() = Some(handle);
        }
        Ok(())
    }

    /// Shut down the streaming pipeline gracefully. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the watcher task panicked.
    pub async fn shutdown(&self) -> Result<(), DbError> {
        if matches!(
            DbState::load(&self.state),
            DbState::Stopped | DbState::ShuttingDown
        ) {
            return Ok(());
        }

        DbState::ShuttingDown.store(&self.state);

        *self.force_ckpt_tx.lock() = None;

        self.shutdown_signal.notify_one();
        if let Some(h) = self.committer_handle.lock().take() {
            h.abort();
        }

        let handle = self.runtime_handle.lock().take();
        if let Some(handle) = handle {
            match tokio::time::timeout(std::time::Duration::from_secs(10), handle).await {
                Ok(Ok(())) => tracing::info!("Pipeline shut down cleanly"),
                Ok(Err(e)) => tracing::warn!(error = %e, "Pipeline task panicked during shutdown"),
                Err(_) => tracing::warn!("Pipeline shutdown timed out after 10s"),
            }
        }

        DbState::Stopped.store(&self.state);
        self.close();
        Ok(())
    }

    /// Stop the streaming pipeline so it can be restarted.
    ///
    /// # Errors
    /// Returns [`DbError::InvalidOperation`] if the pipeline is still starting
    /// or the coordinator does not exit within the stop timeout.
    pub async fn stop_pipeline(&self) -> Result<(), DbError> {
        match DbState::compare_exchange(DbState::Running, DbState::ShuttingDown, &self.state) {
            Ok(_) => {}
            Err(DbState::Created | DbState::Stopped) => return Ok(()),
            Err(_) => {
                return Err(DbError::InvalidOperation(
                    "cannot stop pipeline: not running (starting, or a stop already in progress)"
                        .into(),
                ));
            }
        }

        *self.force_ckpt_tx.lock() = None;

        self.shutdown_signal.notify_one();
        if let Some(h) = self.committer_handle.lock().take() {
            h.abort();
        }

        let handle = self.runtime_handle.lock().take();
        if let Some(handle) = handle {
            match tokio::time::timeout(std::time::Duration::from_secs(10), handle).await {
                Ok(Ok(())) => tracing::info!("Pipeline stopped cleanly"),
                Ok(Err(e)) => tracing::warn!(error = %e, "Pipeline task panicked during stop"),
                Err(_) => {
                    tracing::error!("Pipeline stop timed out after 10s; coordinator still running");
                    return Err(DbError::InvalidOperation(
                        "pipeline stop timed out; coordinator did not exit".into(),
                    ));
                }
            }
        }

        *self.control_tx.lock() = None;
        DbState::Created.store(&self.state);
        Ok(())
    }
}

#[cfg(test)]
mod resolver_tests {
    use super::resolve_stream_output_schemas;
    use crate::connector_manager::StreamRegistration;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;
    use std::time::Duration;

    fn ctx_with_payments() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("method", DataType::Utf8, false),
            Field::new("amount_usd", DataType::Float64, false),
            Field::new("status", DataType::Utf8, false),
            Field::new(
                "event_time",
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                false,
            ),
        ]));
        ctx.register_table("payments", Arc::new(EmptyTable::new(schema)))
            .unwrap();
        ctx.register_udf(datafusion_expr::ScalarUDF::from(
            laminar_sql::datafusion::TumbleWindowStart::new(),
        ));
        ctx
    }

    fn reg(name: &str, sql: &str, windowed: bool) -> StreamRegistration {
        StreamRegistration {
            name: name.to_string(),
            query_sql: sql.to_string(),
            emit_clause: None,
            // Resolver only checks `is_some()`; the size doesn't matter.
            window_config: windowed.then(|| {
                laminar_sql::translator::WindowOperatorConfig::tumbling(
                    "event_time".into(),
                    Duration::ZERO,
                )
            }),
            order_config: None,
            join_config: None,
        }
    }

    #[tokio::test]
    async fn windowed_stream_schema_matches_user_select() {
        let ctx = ctx_with_payments();
        let mut regs = std::collections::HashMap::new();
        regs.insert(
            "agg".to_string(),
            reg(
                "agg",
                "SELECT region, COUNT(*) AS n FROM payments \
                 GROUP BY tumble(event_time, INTERVAL '1' MINUTE), region",
                true,
            ),
        );

        let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
        let names: Vec<&str> = out["agg"]
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["region", "n"]);
    }

    #[tokio::test]
    async fn windowed_stream_with_explicit_window_columns() {
        let ctx = ctx_with_payments();
        ctx.register_udf(datafusion_expr::ScalarUDF::from(
            laminar_sql::datafusion::TumbleWindowEnd::new(),
        ));
        let mut regs = std::collections::HashMap::new();
        regs.insert(
            "agg".to_string(),
            reg(
                "agg",
                "SELECT \
                    tumble(event_time, INTERVAL '1' MINUTE)     AS window_start, \
                    tumble_end(event_time, INTERVAL '1' MINUTE) AS window_end, \
                    region, \
                    COUNT(*) AS n \
                 FROM payments \
                 GROUP BY \
                    tumble(event_time, INTERVAL '1' MINUTE), \
                    tumble_end(event_time, INTERVAL '1' MINUTE), \
                    region",
                true,
            ),
        );

        let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
        let names: Vec<&str> = out["agg"]
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["window_start", "window_end", "region", "n"]);
        assert_eq!(
            out["agg"].field(0).data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            out["agg"].field(1).data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
    }

    #[tokio::test]
    async fn non_windowed_stream_has_no_prefix() {
        let ctx = ctx_with_payments();
        let mut regs = std::collections::HashMap::new();
        regs.insert(
            "passthrough".to_string(),
            reg(
                "passthrough",
                "SELECT region, amount_usd FROM payments",
                false,
            ),
        );

        let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
        let names: Vec<&str> = out["passthrough"]
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["region", "amount_usd"]);
    }

    #[tokio::test]
    async fn chained_streams_resolve_via_iterative_planning() {
        // `b` reads from `a`; iteration order doesn't matter — the loop
        // re-tries `b` after `a` is registered.
        let ctx = ctx_with_payments();
        let mut regs = std::collections::HashMap::new();
        regs.insert(
            "b".to_string(),
            reg("b", "SELECT region, n + 1 AS n_plus_one FROM a", false),
        );
        regs.insert(
            "a".to_string(),
            reg(
                "a",
                "SELECT region, COUNT(*) AS n FROM payments GROUP BY region",
                false,
            ),
        );

        let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
        let b_names: Vec<&str> = out["b"]
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(b_names, vec!["region", "n_plus_one"]);

        // Placeholders must not leak into the public ctx — `subscribe()`
        // is the data path for streams; `SELECT * FROM <stream>` should
        // not silently return zero rows from a left-over EmptyTable.
        assert!(!ctx.table_exist("a").unwrap_or(false));
        assert!(!ctx.table_exist("b").unwrap_or(false));
    }

    #[tokio::test]
    async fn unresolvable_streams_surface_planner_error() {
        let ctx = ctx_with_payments();
        let mut regs = std::collections::HashMap::new();
        // Cycle: a→b, b→a. Planning stalls; we report the unresolved set.
        regs.insert("a".to_string(), reg("a", "SELECT * FROM b", false));
        regs.insert("b".to_string(), reg("b", "SELECT * FROM a", false));

        let err = resolve_stream_output_schemas(&ctx, &regs)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("unresolvable stream dependency"), "got: {err}");
        assert!(err.contains('a') && err.contains('b'), "got: {err}");
    }
}
