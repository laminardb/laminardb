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
                    let obj_store = laminar_storage::object_store_factory::build_object_store(
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

            // Wire per-core WAL for crash recovery between checkpoints.
            // Without this, all data since the last checkpoint is lost on crash.
            let wal_dir = cp_config
                .data_dir
                .clone()
                .or_else(|| self.config.storage_dir.clone())
                .unwrap_or_else(|| std::path::PathBuf::from("./data"))
                .join("wal");
            // Match the TPC runtime's core count logic: explicit config,
            // available_parallelism for external connectors, or source count.
            let num_cores = self
                .config
                .tpc
                .as_ref()
                .and_then(|t| t.num_cores)
                .unwrap_or_else(|| {
                    if has_external {
                        std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
                    } else {
                        source_regs.len().max(1)
                    }
                });
            match laminar_storage::per_core_wal::PerCoreWalManager::new(
                laminar_storage::per_core_wal::PerCoreWalConfig::new(&wal_dir, num_cores),
            ) {
                Ok(wal) => {
                    tracing::info!(
                        wal_dir = %wal_dir.display(),
                        num_cores,
                        "WAL manager initialized"
                    );
                    coord.register_wal_manager(wal);
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "WAL initialization failed — running without WAL \
                         (data between checkpoints may be lost on crash)"
                    );
                }
            }

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
    /// The pipeline uses a thread-per-core architecture: each source
    /// runs on a dedicated I/O thread, pushing events through SPSC
    /// queues to CPU-pinned core threads. A `TpcPipelineCoordinator`
    /// tokio task drains core outboxes and runs SQL cycles.
    ///
    /// When `has_external` is false (pure embedded mode), one core thread
    /// per source is used to minimize overhead while providing unified
    /// watermark tracking, error handling, and checkpointing.
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
        use crate::pipeline::{PipelineConfig, SourceRegistration, TpcPipelineCoordinator};
        use crate::stream_executor::StreamExecutor;
        use laminar_connectors::reference::{ReferenceTableSource, RefreshMode};

        // Build StreamExecutor
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);
        executor.set_lookup_registry(Arc::clone(&self.lookup_registry));

        // Register source schemas for ALL sources (both external connectors
        // and catalog-bridge sources) so the executor can create empty
        // placeholder tables when no data arrives in a given cycle.
        for name in source_regs.keys() {
            if let Some(entry) = self.catalog.get_source(name) {
                executor.register_source_schema(name.clone(), entry.schema.clone());
            }
        }

        for reg in stream_regs.values() {
            executor.add_query(
                reg.name.clone(),
                reg.query_sql.clone(),
                reg.emit_clause.clone(),
                reg.window_config.clone(),
                reg.order_config.clone(),
            );
        }

        // Register temporal join tables as Versioned in the lookup registry
        // so that execute_temporal_query() can use persistent versioned state.
        for tcfg in executor.temporal_join_configs() {
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
                let Ok(version_col_idx) =
                    initial_batch.schema().index_of(&tcfg.table_version_column)
                else {
                    if !initial_batch.schema().fields().is_empty() {
                        tracing::warn!(
                            table=%tcfg.table_name,
                            version_col=%tcfg.table_version_column,
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
                            version_column: tcfg.table_version_column.clone(),
                            stream_time_column: tcfg.stream_time_column.clone(),
                        },
                    );
                    continue;
                };
                let index = Arc::new(
                    laminar_sql::datafusion::lookup_join_exec::VersionedIndex::build(
                        &initial_batch,
                        &key_indices,
                        version_col_idx,
                    )
                    .unwrap_or_default(),
                );
                self.lookup_registry.register_versioned(
                    &tcfg.table_name,
                    laminar_sql::datafusion::VersionedLookupState {
                        batch: initial_batch,
                        index,
                        key_columns,
                        version_column: tcfg.table_version_column.clone(),
                        stream_time_column: tcfg.stream_time_column.clone(),
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
                executor.register_source_schema(name.clone(), entry.schema.clone());
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
            let exactly_once = sink.capabilities().exactly_once;
            let handle = crate::sink_task::SinkTaskHandle::spawn(name.clone(), sink, exactly_once);
            sinks.push((
                name.clone(),
                handle,
                reg.filter_expr.clone(),
                reg.input.clone(),
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
                for (name, handle, _, _) in &sinks {
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
                        // Restore stream executor aggregate state
                        if let Some(op) = recovered.manifest.operator_states.get("stream_executor")
                        {
                            if let Some(bytes) = op.decode_inline() {
                                match executor.restore_state(&bytes) {
                                    Ok(n) => {
                                        tracing::info!(
                                            queries = n,
                                            "Restored stream executor state from checkpoint"
                                        );
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "Stream executor state restore failed, starting fresh"
                                        );
                                    }
                                }
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
        let pipeline_config = PipelineConfig {
            max_poll_records: max_poll,
            channel_capacity: 64,
            fallback_poll_interval: if has_external {
                std::time::Duration::from_millis(10)
            } else {
                std::time::Duration::from_millis(1)
            },
            checkpoint_interval,
            batch_window: if has_external {
                std::time::Duration::from_millis(5)
            } else {
                std::time::Duration::ZERO
            },
            barrier_alignment_timeout: std::time::Duration::from_secs(30),
            delivery_guarantee: self.config.delivery_guarantee,
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
                for (name, handle, _, _) in &sinks {
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
                let has_eo_sink = sinks.iter().any(|(_, h, _, _)| h.exactly_once());
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
        let pipeline_watermark = Arc::clone(&self.pipeline_watermark);
        let checkpoint_in_progress = Arc::new(std::sync::atomic::AtomicBool::new(false));
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

        let callback = crate::pipeline_callback::ConnectorPipelineCallback {
            executor,
            stream_sources,
            sinks,
            watermark_states,
            source_entries_for_wm,
            source_ids,
            tracker,
            counters,
            pipeline_watermark,
            checkpoint_in_progress,
            coordinator,
            table_sources,
            table_store: table_store_for_loop,
            lookup_registry: Arc::clone(&self.lookup_registry),
            filter_ctx: laminar_sql::create_session_context(),
            last_checkpoint: std::time::Instant::now(),
            checkpoint_interval: self
                .config
                .checkpoint
                .as_ref()
                .and_then(|c| c.interval_ms)
                .map(std::time::Duration::from_millis),
            pipeline_hash,
            delivery_guarantee: pipeline_config.delivery_guarantee,
        };

        // Build TPC config (use explicit settings or auto-detect defaults).
        {
            use laminar_core::tpc::TpcConfig;

            let tpc_cfg = self.config.tpc.clone().unwrap_or_default();
            let num_sources = sources.len().max(1);
            let num_cores = tpc_cfg.num_cores.unwrap_or_else(|| {
                if has_external {
                    std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
                } else {
                    // Pure embedded mode: one core per source.
                    num_sources
                }
            });
            // Ensure at least one core per source — SPSC queues require
            // exactly one producer thread. When num_cores < num_sources,
            // round-robin routing puts multiple producers on the same inbox.
            let num_cores = num_cores.max(num_sources);
            if let Some(configured) = tpc_cfg.num_cores {
                if configured < num_sources {
                    tracing::warn!(
                        configured_cores = configured,
                        required_cores = num_sources,
                        "Overriding num_cores to match source count \
                         (SPSC single-producer invariant)"
                    );
                }
            }
            let tpc_config = TpcConfig {
                num_cores,
                cpu_pinning: tpc_cfg.cpu_pinning,
                cpu_start: tpc_cfg.cpu_start,
                numa_aware: tpc_cfg.numa_aware,
                ..Default::default()
            };

            let tpc_coordinator = TpcPipelineCoordinator::new(
                sources,
                pipeline_config,
                &tpc_config,
                Arc::clone(&shutdown),
            )?;

            let handle = tokio::spawn(async move {
                tpc_coordinator.run(Box::new(callback)).await;
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
