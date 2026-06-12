//! Pipeline lifecycle management: start, close, shutdown.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use laminar_core::streaming;
use rustc_hash::FxHashMap;

use crate::db::{DbState, LaminarDB, SourceWatermarkState};
use crate::error::DbError;

/// Resolves each `CREATE STREAM`'s output Arrow schema by planning its SQL.
/// Temporary `EmptyTable` placeholders let downstream streams plan against
/// upstream streams; they are removed before returning.
/// Resolve a query's output schema by planning it. `DataFusion` can't lower
/// `ASOF JOIN`, so on failure retry with the schema-equivalent rewrite. `None`
/// if it still can't be planned (e.g. a dependency isn't registered yet).
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
    // Placeholders we own and must clean up; pre-existing tables are left alone.
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
                // Report the rewritten-plan error when the query is an ASOF join:
                // the raw plan only ever says "AsOf unsupported", which masks the
                // real blocker (the rewrite is what we actually plan against).
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

    /// Returns `true` if the database has been shut down.
    pub fn is_closed(&self) -> bool {
        self.shutdown.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn is_pipeline_running(&self) -> bool {
        matches!(
            DbState::load(&self.state),
            DbState::Running | DbState::Starting | DbState::ShuttingDown
        )
    }

    /// Start the streaming pipeline. Idempotent if already running.
    ///
    /// Activates registered connectors and spawns the background processing
    /// task. On failure the instance is unwound back to `Created` so the
    /// caller can retry after fixing the offending config.
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
            DbState::Created => {}
        }

        DbState::Starting.store(&self.state);

        // Fallback for embedded use without a server.
        {
            let mut guard = self.engine_metrics.lock();
            if guard.is_none() {
                *guard = Some(Arc::new(crate::engine_metrics::EngineMetrics::new(
                    &prometheus::Registry::new(),
                )));
            }
        }

        // Rebuild any catalog objects this node lacks from the shared manifest
        // before wiring the pipeline — replaying DDL here is the same pre-start
        // path users take when they CREATE before start(). Best-effort/no-op
        // outside cluster mode or with no manifest stored.
        #[cfg(feature = "cluster")]
        self.restore_catalog_from_manifest().await;

        match self.start_inner().await {
            Ok(()) => {
                DbState::Running.store(&self.state);
                Ok(())
            }
            Err(e) => {
                // Unwind so a retry actually re-runs startup instead of
                // silently succeeding against a wedged Starting state.
                DbState::Created.store(&self.state);
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn start_inner(&self) -> Result<(), DbError> {
        // Snapshot connector registrations under the lock
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

        // Log which sources have external connectors for debugging.
        for (name, reg) in &source_regs {
            tracing::debug!(source = %name, connector_type = ?reg.connector_type, "Registered source");
        }
        for (name, reg) in &sink_regs {
            tracing::debug!(sink = %name, connector_type = ?reg.connector_type, "Registered sink");
        }

        // Initialize checkpoint coordinator (shared across all pipeline modes)
        if let Some(ref cp_config) = self.config.checkpoint {
            use crate::checkpoint_coordinator::{
                CheckpointConfig as CkpConfig, CheckpointCoordinator,
            };

            let max_retained = cp_config.max_retained.unwrap_or(3);
            let vnode_count = self.vnode_registry.lock().as_ref().map_or(
                laminar_core::storage::checkpoint_manifest::DEFAULT_VNODE_COUNT,
                |r| u16::try_from(r.vnode_count()).unwrap_or(u16::MAX),
            );

            // Resolve once; both the checkpoint store and the decision
            // store want the same root.
            let data_dir = cp_config
                .data_dir
                .clone()
                .or_else(|| self.config.storage_dir.clone())
                .unwrap_or_else(|| std::path::PathBuf::from("./data"));

            let (store, decision_backing): (
                Box<dyn laminar_core::storage::CheckpointStore>,
                Arc<dyn object_store::ObjectStore>,
            ) = if let Some(ref url) = self.config.object_store_url {
                // The builder roots the store at the URL's path prefix,
                // so the checkpoint store (and the decision store
                // sharing `obj`) need no extra key prefix.
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

            // Cluster mode: install the controller so the coordinator
            // can consult it for leader / follower role once the
            // barrier-protocol flow change lands.
            #[cfg(feature = "cluster")]
            if let Some(controller) = self.cluster_controller.lock().clone() {
                coord.set_cluster_controller(controller);
            }

            // Durability gate wiring: if both the state backend and vnode
            // registry are installed, tell the coordinator which vnodes
            // this instance owns. In cluster mode the owner id comes from
            // the cluster controller; in single-instance mode we assume
            // a `single_owner` registry and pass `NodeId(0)`.
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
                // Both sides of the split-brain guard must see the same
                // generation. The coordinator stamps outgoing writes with
                // it (caller side), and the backend rejects incoming
                // writes below it (authority side).
                // Without the backend call the authoritative version
                // stays at 0 and the fence is a silent no-op — the
                // registry's version carries the snapshot generation
                // across a restart, but only in-memory.
                let version = registry.assignment_version();
                backend.set_authoritative_version(version);
                coord.set_state_backend(backend);
                coord.set_assignment_version(version);
                coord.set_vnode_set(laminar_core::state::owned_vnodes(&registry, owner));
                // Leader's gate checks the full registry — across all
                // instances — so the 2PC commit only fires when every
                // follower has also persisted its markers.
                coord.set_gate_vnode_set((0..registry.vnode_count()).collect());
            }

            // Decision store: caller-supplied (cluster) wins, else
            // auto-wire one on the same backing so recovery can read
            // the commit marker.
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

            // Reconcile any Pending sinks from the last manifest against
            // the durable commit marker before accepting new traffic.
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
    ///
    /// Handles both embedded (in-memory only) and connector-backed sources
    /// through a single code path. Connector-less sources are wrapped as
    /// `CatalogSourceConnector` to participate in the pipeline alongside
    /// external connectors (Kafka, CDC, etc.).
    ///
    /// Each source runs as a tokio task pushing batches via mpsc channel
    /// to a `StreamingCoordinator` that drives SQL cycles, sink writes,
    /// and checkpoint coordination.
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

        // Build OperatorGraph context mirroring the rules + partition
        // count installed on `LaminarDB::ctx` via the builder.
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

        // Register lookup/reference tables in the operator graph's
        // SessionContext so JOIN queries can resolve them.
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

        // Install the cluster row-shuffle config if all the pieces are
        // present (registry, sender, receiver, controller). Without any
        // one of them, aggregate queries run single-node.
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
                // Share the DB's staged rehydration map so the graph applies
                // rebalanced-in vnode state into operators each cycle.
                graph.set_rehydration_handle(Arc::clone(&self.rehydrated_vnode_state));
            }
        }

        // Register source schemas for ALL sources (both external connectors
        // and catalog-bridge sources) so the graph can create empty
        // placeholder tables when no data arrives in a given cycle.
        for name in source_regs.keys() {
            if let Some(entry) = self.catalog.get_source(name) {
                graph.register_source_schema(name.clone(), entry.schema.clone());
            }
        }

        // Partial (on-demand) lookup tables: route their enrichment joins to the
        // async-decoupled operator. A table is partial iff it refreshes Manually
        // — the same condition the on-demand phase below uses to register it as
        // `RegisteredLookup::Partial`. Map each to its column names so the
        // operator can disambiguate colliding output columns.
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
        // Ring-1 workers (lookup-enrich fetch, AI) spawn on the main runtime.
        graph.set_runtime_handle(
            self.ai_handle
                .clone()
                .unwrap_or_else(tokio::runtime::Handle::current),
        );

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
        // Surface any plan-time AI routing errors (unknown model, unsupported
        // task, malformed AI query) collected during `add_query`.
        graph.take_build_errors()?;

        // Register temporal join tables as Versioned in the lookup registry
        // so that temporal join operators can use persistent versioned state.
        for tcfg in graph.temporal_join_configs() {
            if self.lookup_registry.get_entry(&tcfg.table_name).is_none() {
                // Get initial data. If none exists yet, use an empty batch
                // with the correct schema from the catalog (not Schema::empty).
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

                // When table_version_column is empty (translator couldn't resolve it
                // from the AS OF clause), pick the first timestamp/int column that
                // isn't the join key.
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
                    // Register with empty index — built on first CDC update.
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

        // Grab the shared Prometheus registry (if set) so connectors can
        // register their metrics on it.
        let prom_registry = self.prometheus_registry.lock().clone();

        // Build sources as owned SourceRegistrations (no Arc<Mutex>).
        let mut sources: Vec<SourceRegistration> = Vec::new();
        for (name, reg) in &source_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let mut config = build_source_config(reg)?;

            // Pass the SQL-defined Arrow schema to the connector so it can
            // deserialize records with the correct column names and types.
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
            // Cluster mode: hand the source the vnode topology so a partitioned
            // source (Kafka) consumes only the partitions this node owns and
            // re-binds on rotation. No-op for non-partitioned sources.
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
            // Property-driven watermark wiring. `[source.watermark]` in
            // TOML is the preferred path (honours max_out_of_orderness);
            // this exists for sources that pass `event.time.column` /
            // `event.time.field` as a connector property instead. The
            // second pass below builds the matching SourceWatermarkState.
            // Kafka uses `column`, WebSocket uses `field` — both spellings
            // are live.
            if let Some(entry) = self.catalog.get_source(name) {
                if entry.source.event_time_column().is_none() {
                    if let Some(col) = config.get("event.time.column") {
                        entry.source.set_event_time_column(col);
                    } else if let Some(col) = config.get("event.time.field") {
                        entry.source.set_event_time_column(col);
                    }
                }
                // Carry the out-of-orderness bound alongside the column so
                // the fallback watermark path below uses the configured
                // tolerance instead of Duration::ZERO.
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

        // Bridge connector-less sources into the pipeline so db.insert()
        // data flows through the standard source task → coordinator path.
        // This covers two cases:
        //   1. Sources in source_regs with connector_type == None (registered
        //      in connector manager but without a FROM clause).
        //   2. Sources in the catalog but NOT in source_regs at all (pure
        //      embedded sources created without any connector specification).
        let bridged_names: rustc_hash::FxHashSet<String> =
            sources.iter().map(|s| s.name.clone()).collect();
        // First: bridge sources in source_regs that have no connector.
        for (name, reg) in &source_regs {
            if reg.connector_type.is_some() {
                continue; // Already created as external connector above
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
        // Second: bridge catalog sources not in source_regs (embedded-only
        // sources that were never registered with the connector manager).
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

        // Resolve each stream's output schema with DataFusion so sinks
        // downstream see it via `_arrow_schema` (mirrors the source path
        // above). Publish the result into `LaminarDB::stream_schemas` so
        // SUBSCRIBE WHERE can compile predicates against the real schema.
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

        // Build sinks. Each runs in its own tokio task with a bounded
        // command channel and shares one event channel back to the
        // pipeline callback.
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
            // Resolve the upstream schema from a stream first, then fall back
            // to a source — `CREATE SINK ... FROM <name>` accepts either.
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
            // Open the connector before handing it to the task.
            sink.open(&config)
                .await
                .map_err(|e| DbError::Connector(format!("Failed to open sink '{name}': {e}")))?;
            let caps = sink.capabilities();
            // Resolve per-sink write timeout: user override (property)
            // takes precedence over the sink's declared suggestion.
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
        // Drop the local sender so the channel disconnects when all
        // sink tasks exit.
        drop(sink_event_tx);

        // Build table sources from registrations
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

        // Register sinks with the checkpoint coordinator.
        // Sources are owned by the TPC runtime — checkpoint reads go
        // through lock-free watch channels instead.
        {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                for (name, handle, _, _, _) in &sinks {
                    let exactly_once = handle.exactly_once();
                    coord.register_sink(name.clone(), handle.clone(), exactly_once);
                }
            }
        }

        // Recovery: restore sink/table state via unified coordinator.
        // Must run BEFORE begin_initial_epoch so the coordinator's epoch
        // reflects the recovered state. We also hoist the recovered
        // per-source watermarks out of the coordinator lock so the later
        // watermark-state construction can seed each generator + the
        // combined tracker. Without this, generators restart at
        // `i64::MIN` while source offsets resume mid-stream — windowed
        // operators re-fire and late-drop diverges from the pre-crash
        // run, breaking deterministic recovery for every event-time
        // pipeline.
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
                        // Attach recovered source offsets to SourceRegistrations.
                        // These will be passed to the TPC source adapters, which
                        // call connector.restore() after open() to seek Kafka
                        // consumers to their checkpoint positions.
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
                                        // Source offsets were already staged
                                        // for the connectors above. Resuming
                                        // with empty operator state would
                                        // silently lose every in-flight
                                        // window / aggregator. Fail loud
                                        // instead — the operator can drop
                                        // the checkpoint and restart fresh
                                        // explicitly if that's the intent.
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

                        // Skip MV restore when operator state failed to load —
                        // stale MV data with fresh operators is inconsistent.
                        // No operator state at all (stateless pipeline) is fine.
                        if !graph_restore_failed {
                            let prefix = crate::mv_store::CHECKPOINT_KEY_PREFIX;
                            let mut store = self.mv_store.write();
                            let mut restored = 0usize;
                            for (key, op) in &recovered.manifest.operator_states {
                                if let Some(name) = key.strip_prefix(prefix) {
                                    if let Some(bytes) = op.decode_inline() {
                                        match store.restore_from_ipc(name, &bytes) {
                                            Ok(true) => restored += 1,
                                            Ok(false) => {} // MV no longer registered
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

        // Begin the initial epoch on exactly-once sinks AFTER recovery
        // so the coordinator's epoch reflects the recovered checkpoint.
        {
            let guard = self.coordinator.lock().await;
            if let Some(ref coord) = *guard {
                coord.begin_initial_epoch().await?;
            }
        }

        // Snapshot phase: populate tables before stream processing begins
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
            // Update lookup registry so join queries see fresh data.
            // Skip if already registered as Versioned (temporal join tables
            // must keep their version history, not be overwritten as Snapshot).
            if matches!(
                self.lookup_registry.get_entry(name),
                Some(laminar_sql::datafusion::RegisteredLookup::Versioned(_))
            ) {
                // Already versioned — don't downgrade to Snapshot.
            } else if let Some(batch) = self.table_store.read().to_record_batch(name) {
                self.lookup_registry
                    .register(name, laminar_sql::datafusion::LookupSnapshot { batch });
            }
        }

        // On-demand (Manual) tables: register as Partial in the lookup
        // registry so lookup joins use the lookup cache + source fallback path,
        // then promote to SnapshotPlusCdc so poll_tables() calls poll_changes().
        for (name, _source, mode) in &mut table_sources {
            if !matches!(mode, RefreshMode::Manual) {
                continue;
            }
            let Some(reg) = table_regs.get(name.as_str()) else {
                continue;
            };
            // 64 MiB default budget; the cache is byte-weighted, not entry-counted.
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
            // Try to create a lookup source for cache-miss fallback via
            // the registry factory (no cross-crate type dependency).
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

            // Projection pushdown: fetch only the columns any query references
            // (a per-table superset, since the cache is shared), plus the key.
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

        // Get stream source handles so results also flow to db.subscribe().
        let mut stream_sources: Vec<(String, streaming::Source<crate::catalog::ArrowRecord>)> =
            Vec::new();
        for reg in stream_regs.values() {
            if let Some(src) = self.catalog.get_stream_source(&reg.name) {
                stream_sources.push((reg.name.clone(), src));
            }
        }

        // Per-source watermark state. Future-skew ceiling;
        // `LAMINAR_MAX_FUTURE_SKEW_MS=0` disables it (legacy unbounded).
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

        // Fallback watermark path for sources that set `event_time_column`
        // without an SQL `WATERMARK FOR` clause — exercised by the
        // programmatic API (`handle.set_event_time_column`) and by the
        // connector-property wiring above. Uses the bound from
        // `source.max_out_of_orderness()` if one was wired from connector
        // properties (e.g. Kafka `max.out.of.orderness.ms`), otherwise
        // falls back to `Duration::ZERO`.
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

        // Idle-source detection off by default (opt in via
        // LAMINAR_SOURCE_IDLE_TIMEOUT_MS > 0, applied to all sources;
        // unset/0/invalid ⇒ disabled). Tracker is per-source capable.
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

        // A pipeline with some watermarked and some un-watermarked sources
        // is a semantic hazard: an un-watermarked source's per-stream
        // watermark falls back to the global, so a join/window over both
        // closes on the *watermarked* source's clock. Surface this so it
        // isn't an invisible source of "where did my late rows go?"
        // bug reports. The real fix needs planner-level rejection.
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

        // Seed every restored generator and the combined tracker with the
        // watermark captured at checkpoint time. Anything not in the
        // recovered map (or `i64::MIN`) starts cold, which is correct for
        // first-run / fresh sources.
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

        // Build pipeline config.
        // Embedded mode (no external connectors): zero batch window for
        // minimal latency — data is processed as soon as it arrives.
        // Connector mode: 5ms batch window amortizes SQL overhead across
        // high-throughput external sources (Kafka, CDC).
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
            // cycle_budget is a soft cap for logging; ensure it's at least
            // drain + query so sub-budgets can actually be used.
            cycle_budget_ns: 10_000_000_u64.max(drain_budget_ns + query_budget_ns),
            drain_budget_ns,
            query_budget_ns,
            background_budget_ns: 5_000_000, // 5ms
            max_input_buf_batches: self.config.pipeline_max_input_buf_batches.unwrap_or(256),
            max_input_buf_bytes: self.config.pipeline_max_input_buf_bytes,
            backpressure_policy: self.config.pipeline_backpressure_policy,
        };

        // Validate delivery guarantee constraints.
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

        // Build the PipelineCallback implementation that bridges to db.rs internals.
        let pipeline_watermark = Arc::clone(&self.pipeline_watermark);
        let coordinator = Arc::clone(&self.coordinator);
        let table_store_for_loop = self.table_store.clone();
        // Compute a pipeline hash for change detection across checkpoints.
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

        // Wire per-query budget and input buffer cap from pipeline config.
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

        // Force-checkpoint channel: `db.checkpoint()` on the caller side
        // hands a oneshot sender across to the callback, which captures
        // operator state and replies. Installed here so both ends exist
        // before the compute thread spawns. Crossfire for consistency
        // with the rest of the pipeline plumbing (sink_task, coordinator).
        let (force_ckpt_tx, force_ckpt_rx) = crossfire::mpsc::bounded_async::<
            crate::db::ForceCheckpointReply,
        >(crate::db::FORCE_CHECKPOINT_CHANNEL_CAPACITY);
        *self.force_ckpt_tx.lock() = Some(force_ckpt_tx);

        let (checkpoint_complete_tx, checkpoint_complete_rx) = crossfire::mpsc::bounded_async::<(
            u64,
            rustc_hash::FxHashMap<String, laminar_connectors::checkpoint::SourceCheckpoint>,
        )>(16);
        // Shared admission state: the callback claims a slot (and staged
        // bytes) per in-flight epoch; the streaming coordinator gates new
        // barriers on the caps. A single-open-transaction sink cannot
        // overlap epochs, so depth is capped at 1 whenever ANY registered
        // sink is exactly-once — not just when the pipeline-level
        // guarantee says so. (DDL/server-configured sinks declare
        // exactly-once via connector capabilities without ever setting
        // the pipeline-level guarantee; keying off the pipeline config
        // alone would pipeline epochs over an open Kafka transaction.)
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
                        cfg.max_staged_bytes,
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
        };

        // Start the streaming coordinator on a dedicated compute thread.
        // Source tasks were already spawned on the main tokio runtime in
        // StreamingCoordinator::new(). The coordinator communicates with
        // them via crossfire mpsc which works across runtimes.
        {
            // Control channel for live DDL (add/drop stream).
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
                        // done_tx dropped → done_rx returns Err → logged by watcher task
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

            // Wait for the thread to confirm the runtime started.
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
            let handle = tokio::spawn(async move {
                if done_rx.await.is_err() {
                    tracing::error!("laminar-compute thread exited unexpectedly");
                    DbState::Stopped.store(&watcher_state);
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
    /// Returns an error if shutdown encounters an error.
    pub async fn shutdown(&self) -> Result<(), DbError> {
        if matches!(
            DbState::load(&self.state),
            DbState::Stopped | DbState::ShuttingDown
        ) {
            return Ok(());
        }

        DbState::ShuttingDown.store(&self.state);

        // Drop the force-checkpoint sender first so any later db.checkpoint()
        // errors cleanly instead of waiting on a oneshot that will never reply.
        *self.force_ckpt_tx.lock() = None;

        self.shutdown_signal.notify_one();

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
        // Atomically claim the stop: only the caller that flips Running ->
        // ShuttingDown tears down, so a concurrent stop can't race in and mark
        // the pipeline restartable while the coordinator is still shutting down.
        match DbState::compare_exchange(DbState::Running, DbState::ShuttingDown, &self.state) {
            Ok(_) => {}
            // Already stopped / never started — idempotent no-op.
            Err(DbState::Created | DbState::Stopped) => return Ok(()),
            // Starting, or a stop is already in progress — refuse.
            Err(_) => {
                return Err(DbError::InvalidOperation(
                    "cannot stop pipeline: not running (starting, or a stop already in progress)"
                        .into(),
                ));
            }
        }

        // Clear the force-checkpoint sender.
        *self.force_ckpt_tx.lock() = None;

        self.shutdown_signal.notify_one();

        // Take runtime handle before awaiting.
        let handle = self.runtime_handle.lock().take();
        if let Some(handle) = handle {
            match tokio::time::timeout(std::time::Duration::from_secs(10), handle).await {
                Ok(Ok(())) => tracing::info!("Pipeline stopped cleanly"),
                Ok(Err(e)) => tracing::warn!(error = %e, "Pipeline task panicked during stop"),
                Err(_) => {
                    // Coordinator did not exit, keep state as ShuttingDown.
                    tracing::error!("Pipeline stop timed out after 10s; coordinator still running");
                    return Err(DbError::InvalidOperation(
                        "pipeline stop timed out; coordinator did not exit".into(),
                    ));
                }
            }
        }

        // Drop control channel and reset state to allow restart.
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
