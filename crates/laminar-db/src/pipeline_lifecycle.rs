//! Pipeline lifecycle management: start, close, shutdown.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use laminar_core::streaming;
use rustc_hash::FxHashMap;

use crate::db::{
    LaminarDB, SourceWatermarkState, STATE_CREATED, STATE_RUNNING, STATE_SHUTTING_DOWN,
    STATE_STARTING, STATE_STOPPED,
};
use crate::error::DbError;

pub(crate) fn url_to_checkpoint_prefix(url: &str) -> String {
    // Strip scheme
    let after_scheme = url.find("://").map_or(url, |i| &url[i + 3..]);

    // For file:// URLs, the prefix is empty (LocalFileSystem already has the root)
    if url.starts_with("file://") {
        return String::new();
    }

    // For cloud URLs like s3://bucket/prefix → extract everything after bucket
    if let Some(slash_pos) = after_scheme.find('/') {
        let prefix = &after_scheme[slash_pos + 1..];
        if prefix.is_empty() {
            String::new()
        } else if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        }
    } else {
        String::new()
    }
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
        let s = self.state.load(std::sync::atomic::Ordering::Acquire);
        s == STATE_RUNNING || s == STATE_STARTING || s == STATE_SHUTTING_DOWN
    }

    /// Start the streaming pipeline.
    ///
    /// Activates all registered connectors and begins processing.
    /// This is a no-op if the pipeline is already running.
    ///
    /// When the `kafka` feature is enabled and Kafka sources/sinks are
    /// registered, this builds `KafkaSource`/`KafkaSink` instances and
    /// spawns a background task that polls sources, executes stream queries
    /// via `DataFusion`, and writes results to sinks.
    ///
    /// In embedded (in-memory) mode, this simply transitions to `Running`.
    ///
    /// # Errors
    ///
    /// Returns an error if the pipeline cannot be started. On failure, the
    /// instance is unwound back to `STATE_CREATED` so the caller can retry
    /// after fixing the offending config.
    pub async fn start(&self) -> Result<(), DbError> {
        let current = self.state.load(std::sync::atomic::Ordering::Acquire);
        if current == STATE_RUNNING || current == STATE_STARTING {
            return Ok(());
        }
        if current == STATE_STOPPED {
            return Err(DbError::InvalidOperation(
                "Cannot start a stopped pipeline. Create a new LaminarDB instance.".into(),
            ));
        }

        self.state
            .store(STATE_STARTING, std::sync::atomic::Ordering::Release);

        // Fallback for embedded use without a server.
        {
            let mut guard = self.engine_metrics.lock();
            if guard.is_none() {
                *guard = Some(Arc::new(crate::engine_metrics::EngineMetrics::new(
                    &prometheus::Registry::new(),
                )));
            }
        }

        match self.start_inner().await {
            Ok(()) => {
                self.state
                    .store(STATE_RUNNING, std::sync::atomic::Ordering::Release);
                Ok(())
            }
            Err(e) => {
                // Any failure between STATE_STARTING and STATE_RUNNING must
                // unwind — otherwise the instance is wedged and a retry from
                // the caller silently succeeds without actually starting.
                self.state
                    .store(STATE_CREATED, std::sync::atomic::Ordering::Release);
                Err(e)
            }
        }
    }

    /// Runs the actual startup sequence. Called exclusively by
    /// [`LaminarDB::start`], which handles the `STATE_STARTING` ↔
    /// `STATE_RUNNING` / `STATE_CREATED` transitions around it.
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
            let vnode_count = self
                .vnode_registry
                .lock()
                .as_ref()
                .map(|r| u16::try_from(r.vnode_count()).unwrap_or(u16::MAX))
                .unwrap_or(laminar_storage::checkpoint_manifest::DEFAULT_VNODE_COUNT);

            let store: Box<dyn laminar_storage::CheckpointStore> =
                if let Some(ref url) = self.config.object_store_url {
                    let obj_store = laminar_storage::object_store_builder::build_object_store(
                        url,
                        &self.config.object_store_options,
                    )
                    .map_err(|e| DbError::Config(format!("object store: {e}")))?;
                    let prefix = url_to_checkpoint_prefix(url);
                    Box::new(
                        laminar_storage::checkpoint_store::ObjectStoreCheckpointStore::new(
                            obj_store,
                            prefix,
                            max_retained,
                        )
                        .map_err(|e| DbError::Config(format!("checkpoint store runtime: {e}")))?
                        .with_vnode_count(vnode_count),
                    )
                } else {
                    let data_dir = cp_config
                        .data_dir
                        .clone()
                        .or_else(|| self.config.storage_dir.clone())
                        .unwrap_or_else(|| std::path::PathBuf::from("./data"));
                    Box::new(
                        laminar_storage::checkpoint_store::FileSystemCheckpointStore::new(
                            &data_dir,
                            max_retained,
                        )
                        .with_vnode_count(vnode_count),
                    )
                };

            let config = CkpConfig {
                interval: cp_config.interval_ms.map(std::time::Duration::from_millis),
                max_retained,
                ..CkpConfig::default()
            };
            let mut coord = CheckpointCoordinator::new(config, store);
            if let Some(ref prom) = *self.engine_metrics.lock() {
                coord.set_metrics(Arc::clone(prom));
            }

            // Cluster mode: install the controller so the coordinator
            // can consult it for leader / follower role once the
            // barrier-protocol flow change lands.
            #[cfg(feature = "cluster-unstable")]
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
                    #[cfg(feature = "cluster-unstable")]
                    {
                        self.cluster_controller.lock().as_ref().map_or(
                            laminar_core::state::NodeId(0),
                            |c| laminar_core::state::NodeId(c.instance_id().0),
                        )
                    }
                    #[cfg(not(feature = "cluster-unstable"))]
                    {
                        laminar_core::state::NodeId(0)
                    }
                };
                coord.set_state_backend(backend);
                // Phase 1.4 fence: stamp marker writes with the live
                // registry generation. Zero until a snapshot rotates
                // in; harmless no-op on the backend side.
                coord.set_assignment_version(registry.assignment_version());
                coord.set_vnode_set(laminar_core::state::owned_vnodes(&registry, owner));
                // Leader's gate checks the full registry — across all
                // instances — so the 2PC commit only fires when every
                // follower has also persisted its markers.
                coord.set_gate_vnode_set((0..registry.vnode_count()).collect());
            }

            // Cluster recovery: if this instance is the new leader and
            // the last manifest was a prepared-not-committed epoch,
            // announce Abort so surviving followers roll back.
            #[cfg(feature = "cluster-unstable")]
            coord.reconcile_orphaned_prepare().await;

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

        // Build OperatorGraph context. Mirrors the rules + partition
        // count installed on `LaminarDB::ctx` via the builder so that
        // cluster-mode rules like `DistributedAggregateRule` actually
        // fire each cycle inside the streaming pipeline (not just on
        // `db.execute()` queries against the outer context).
        let ctx = {
            use datafusion::execution::SessionStateBuilder;
            let mut session_config = laminar_sql::datafusion::base_session_config();
            if let Some(n) = self.pipeline_target_partitions {
                session_config = session_config.with_target_partitions(n);
            }
            let mut state_builder = SessionStateBuilder::new()
                .with_config(session_config)
                .with_default_features();
            for rule in self.physical_optimizer_rules.iter() {
                state_builder = state_builder.with_physical_optimizer_rule(Arc::clone(rule));
            }
            datafusion::prelude::SessionContext::new_with_state(state_builder.build())
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

        // Install the cluster row-shuffle config if all the pieces are
        // present (registry, sender, receiver, controller). Without any
        // one of them, aggregate queries run single-node.
        #[cfg(feature = "cluster-unstable")]
        {
            let sender = self.shuffle_sender.lock().clone();
            let receiver = self.shuffle_receiver.lock().clone();
            let registry = self.vnode_registry.lock().clone();
            let controller = self.cluster_controller.lock().clone();
            if let (Some(sender), Some(receiver), Some(registry), Some(controller)) =
                (sender, receiver, registry, controller)
            {
                let self_id =
                    laminar_core::state::NodeId(controller.instance_id().0);
                graph.set_cluster_shuffle(
                    crate::operator::sql_query::ClusterShuffleConfig {
                        registry,
                        sender,
                        receiver,
                        self_id,
                    },
                );
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

        for reg in stream_regs.values() {
            graph.add_query(
                reg.name.clone(),
                reg.query_sql.clone(),
                reg.emit_clause.clone(),
                reg.window_config.clone(),
                reg.order_config.clone(),
                None,
            );
        }

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

            let source = self
                .connector_registry
                .create_source(&config, prom_registry.as_deref())
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Cannot create source '{}' (type '{}'): {e}",
                        name,
                        config.connector_type()
                    ))
                })?;
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
            let config = build_sink_config(reg)?;
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
        // reflects the recovered state.
        {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                match coord.recover().await {
                    Ok(Some(recovered)) => {
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
                            #[cfg(feature = "cluster-unstable")]
                            {
                                self.cluster_controller
                                    .lock()
                                    .as_ref()
                                    .map_or(0, |c| c.instance_id().0)
                            }
                            #[cfg(not(feature = "cluster-unstable"))]
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
                                        graph_restore_failed = true;
                                        tracing::warn!(
                                            error = %e,
                                            "Operator graph state restore failed, starting fresh"
                                        );
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
                self.lookup_registry.register(
                    name,
                    laminar_sql::datafusion::LookupSnapshot {
                        batch,
                        key_columns: vec![], // already indexed by primary key
                    },
                );
            }
        }

        // On-demand (Manual) tables: register as Partial in the lookup
        // registry so lookup joins use the foyer cache + source fallback path,
        // then promote to SnapshotPlusCdc so poll_tables() calls poll_changes().
        for (name, _source, mode) in &mut table_sources {
            if !matches!(mode, RefreshMode::Manual) {
                continue;
            }
            let Some(reg) = table_regs.get(name.as_str()) else {
                continue;
            };
            let max_entries = reg.cache_max_entries.unwrap_or(65_536);
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

            let cache = Arc::new(laminar_core::lookup::foyer_cache::FoyerMemoryCache::new(
                0,
                laminar_core::lookup::foyer_cache::FoyerMemoryCacheConfig {
                    capacity: max_entries,
                    shards: 16,
                },
            ));
            // Try to create a lookup source for cache-miss fallback via
            // the registry factory (no cross-crate type dependency).
            let lookup_source = if let Ok(mut config) = build_table_config(reg) {
                config.set("_primary_key_columns", pk_csv.as_str());
                match self.connector_registry.create_lookup_source(config).await {
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

            self.lookup_registry.register_partial(
                name,
                laminar_sql::datafusion::PartialLookupState {
                    foyer_cache: cache,
                    schema,
                    key_columns: pk_cols,
                    key_sort_fields,
                    source: lookup_source,
                    fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(16)),
                },
            );
            *mode = RefreshMode::SnapshotPlusCdc;
            tracing::info!(
                table = %name,
                max_entries,
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

        // Build per-source watermark tracking state (connector pipeline)
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
                            laminar_core::time::BoundedOutOfOrdernessGenerator::from_duration(dur),
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
                            ),
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

        let tracker = if source_ids.is_empty() {
            None
        } else {
            Some(laminar_core::time::WatermarkTracker::new(source_ids.len()))
        };

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
            // Tracks CheckpointConfig::default().alignment_timeout.
            // TODO: expose alignment_timeout_ms in LaminarDbConfig.checkpoint
            // so users can configure this.
            barrier_alignment_timeout: std::time::Duration::from_secs(30),
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

        let prom = self
            .engine_metrics
            .lock()
            .clone()
            .expect("EngineMetrics must be set before start()");

        // Force-checkpoint channel: `db.checkpoint()` on the caller side
        // hands a oneshot sender across to the callback, which captures
        // operator state and replies. Installed here so both ends exist
        // before the compute thread spawns.
        let (force_ckpt_tx, force_ckpt_rx) = tokio::sync::mpsc::unbounded_channel();
        *self.force_ckpt_tx.lock() = Some(force_ckpt_tx);

        let callback = crate::pipeline_callback::ConnectorPipelineCallback {
            graph,
            stream_sources,
            sinks,
            watermark_states,
            source_entries_for_wm,
            source_ids,
            tracker,
            prom,
            pipeline_watermark,
            coordinator,
            table_sources,
            table_store: table_store_for_loop,
            mv_store: self.mv_store.clone(),
            lookup_registry: Arc::clone(&self.lookup_registry),
            filter_ctx: laminar_sql::create_session_context(),
            compiled_sink_filters: Vec::new(),
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
            #[cfg(feature = "cluster-unstable")]
            cluster_controller: self.cluster_controller.lock().clone(),
            #[cfg(feature = "cluster-unstable")]
            last_follower_epoch: None,
            force_ckpt_rx: Some(force_ckpt_rx),
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
            .await?;

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
                    watcher_state.store(STATE_STOPPED, std::sync::atomic::Ordering::Release);
                    watcher_shutdown.notify_one();
                }
            });

            *self.runtime_handle.lock() = Some(handle);
        }
        Ok(())
    }

    /// Shut down the streaming pipeline gracefully.
    ///
    /// Signals the processing loop to stop, waits for it to complete
    /// (with a timeout), then transitions to `Stopped`.
    /// This is idempotent -- calling it multiple times is safe.
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown encounters an error.
    pub async fn shutdown(&self) -> Result<(), DbError> {
        let current = self.state.load(std::sync::atomic::Ordering::Acquire);
        if current == STATE_STOPPED || current == STATE_SHUTTING_DOWN {
            return Ok(());
        }

        self.state
            .store(STATE_SHUTTING_DOWN, std::sync::atomic::Ordering::Release);

        // Drop the force-checkpoint sender before signalling shutdown.
        // Any subsequent `db.checkpoint()` call falls through to the
        // (now-defunct) coordinator path, which errors cleanly instead
        // of hanging on a oneshot that will never be answered.
        *self.force_ckpt_tx.lock() = None;

        // Signal the runtime loop to stop
        self.shutdown_signal.notify_one();

        // Await the runtime handle (with timeout)
        let handle = self.runtime_handle.lock().take();
        if let Some(handle) = handle {
            match tokio::time::timeout(std::time::Duration::from_secs(10), handle).await {
                Ok(Ok(())) => {
                    tracing::info!("Pipeline shut down cleanly");
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "Pipeline task panicked during shutdown");
                }
                Err(_) => {
                    tracing::warn!("Pipeline shutdown timed out after 10s");
                }
            }
        }

        self.state
            .store(STATE_STOPPED, std::sync::atomic::Ordering::Release);
        self.close();
        Ok(())
    }
}
