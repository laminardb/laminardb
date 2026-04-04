//! Pipeline lifecycle management: start, close, shutdown.
//!
//! Reopened `impl LaminarDB` — split from `db.rs`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use laminar_core::streaming;
use rustc_hash::FxHashMap;

use crate::db::{
    infer_timestamp_format, LaminarDB, SourceWatermarkState, STATE_RUNNING, STATE_SHUTTING_DOWN,
    STATE_STARTING, STATE_STOPPED,
};
use crate::error::DbError;

/// Extract a checkpoint path prefix from an object store URL.
///
/// For `s3://bucket/prefix/path` → `"prefix/path/"`.
/// For `file:///some/path` → `""` (local FS uses the path directly).
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

    /// Check if the database is shut down.
    pub fn is_closed(&self) -> bool {
        self.shutdown.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Check if the streaming pipeline is active (starting, running, or
    /// shutting down). DDL that requires connector instantiation or task
    /// cancellation is unsafe in all three states.
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
    /// Returns an error if the pipeline cannot be started.
    #[allow(clippy::too_many_lines)]
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
                        .map_err(|e| DbError::Config(format!("checkpoint store runtime: {e}")))?,
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
                        ),
                    )
                };

            let config = CkpConfig {
                interval: cp_config.interval_ms.map(std::time::Duration::from_millis),
                max_retained,
                ..CkpConfig::default()
            };
            let mut coord = CheckpointCoordinator::new(config, store);
            coord.set_counters(Arc::clone(&self.counters));

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

        self.state
            .store(STATE_RUNNING, std::sync::atomic::Ordering::Release);
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

        // Build OperatorGraph
        let ctx = laminar_sql::create_session_context();
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
        graph.set_counters(Arc::clone(&self.counters));

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
                .create_source(&config)
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
            // Wire event.time.column from connector config to the core Source
            // so SourceWatermarkState can extract watermarks from batch data.
            if let Some(entry) = self.catalog.get_source(name) {
                if entry.source.event_time_column().is_none() {
                    if let Some(col) = config.get("event.time.column") {
                        entry.source.set_event_time_column(col);
                    } else if let Some(col) = config.get("event.time.field") {
                        entry.source.set_event_time_column(col);
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

        // Build sinks via registry (generic — no connector-specific code).
        // Each sink runs in its own tokio task with a bounded command channel,
        // eliminating Arc<Mutex> contention between pipeline writes and
        // checkpoint operations.
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
            let mut sink = self.connector_registry.create_sink(&config).map_err(|e| {
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
            let handle =
                crate::sink_task::SinkTaskHandle::spawn(name.clone(), sink, caps.exactly_once);
            sinks.push((
                name.clone(),
                handle,
                reg.filter_expr.clone(),
                reg.input.clone(),
                caps.changelog,
            ));
        }

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
                                        if let Err(e) = store.restore_from_ipc(name, &bytes) {
                                            tracing::warn!(mv = name, error = %e, "MV restore failed");
                                        } else {
                                            restored += 1;
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
                    let format = infer_timestamp_format(&entry.schema, col);
                    let extractor =
                        laminar_core::time::EventTimeExtractor::from_column(col, format)
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
                            format,
                        },
                    );
                }
                source_entries_for_wm.insert(name, entry);
            }
        }

        // Also create watermark state for sources that declared event_time_column
        // programmatically (via source.set_event_time_column()) but have no SQL WATERMARK
        for name in self.catalog.list_sources() {
            if watermark_states.contains_key(&name) {
                continue;
            }
            if let Some(entry) = self.catalog.get_source(&name) {
                if let Some(col) = entry.source.event_time_column() {
                    let format = infer_timestamp_format(&entry.schema, &col);
                    let extractor =
                        laminar_core::time::EventTimeExtractor::from_column(&col, format)
                            .with_mode(laminar_core::time::ExtractionMode::Max);
                    let generator: Box<dyn laminar_core::time::WatermarkGenerator> = if entry
                        .is_processing_time
                        .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        Box::new(laminar_core::time::ProcessingTimeGenerator::new())
                    } else {
                        Box::new(
                            laminar_core::time::BoundedOutOfOrdernessGenerator::from_duration(
                                std::time::Duration::ZERO,
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
                            format,
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
            sink_write_timeout: std::time::Duration::from_secs(30),
            max_input_buf_batches: 256,
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
        let counters = Arc::clone(&self.counters);
        // Mirror the configured per-operator state cap into the counters
        // so the /metrics endpoint can report the enforced limit.
        if let Some(cap) = self.config.max_state_bytes_per_operator {
            #[allow(clippy::cast_possible_truncation)]
            counters
                .max_state_bytes
                .store(cap as u64, std::sync::atomic::Ordering::Relaxed);
        }
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

        let callback = crate::pipeline_callback::ConnectorPipelineCallback {
            graph,
            stream_sources,
            sinks,
            watermark_states,
            source_entries_for_wm,
            source_ids,
            tracker,
            counters,
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
            sink_write_timeout: pipeline_config.sink_write_timeout,
            serialization_timeout: std::time::Duration::from_secs(120),
            sink_timed_out: false,
            cycle_histogram: std::cell::RefCell::new(
                crate::checkpoint_coordinator::DurationHistogram::new(),
            ),
        };

        // Start the streaming coordinator on a dedicated compute thread.
        // Source tasks were already spawned on the main tokio runtime in
        // StreamingCoordinator::new(). The coordinator communicates with
        // them via tokio::sync::mpsc which works across runtimes.
        {
            // Control channel for live DDL (add/drop stream).
            let (control_tx, control_rx) =
                tokio::sync::mpsc::channel::<crate::pipeline::ControlMsg>(64);
            *self.control_tx.lock() = Some(control_tx);

            let coordinator = crate::pipeline::StreamingCoordinator::new(
                sources,
                pipeline_config,
                Arc::clone(&shutdown),
                control_rx,
            )
            .await?;

            let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
            let (startup_tx, startup_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
            match std::thread::Builder::new()
                .name("laminar-compute".into())
                .spawn(move || {
                    let rt = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => {
                            let _ = startup_tx.send(Ok(()));
                            rt
                        }
                        Err(e) => {
                            let _ = startup_tx.send(Err(format!("compute runtime: {e}")));
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
                    let _ = done_tx.send(());
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
