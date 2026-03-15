//! `PipelineCallback` implementation bridging the event-driven pipeline
//! coordinator to the stream executor, sinks, watermarks, checkpoints,
//! and table sources.
#![allow(clippy::disallowed_types)] // cold path

use std::sync::Arc;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_common::DFSchema;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_core::streaming;
use rustc_hash::FxHashMap;

use crate::db::{filter_late_rows, SourceWatermarkState};
use crate::error::DbError;

/// Base prefix for the temporary table used by sink WHERE filters.
const FILTER_INPUT_TABLE: &str = "__laminar_filter_input";

/// Monotonic counter for unique filter table names (concurrent-safe).
static FILTER_TABLE_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Implements [`PipelineCallback`](crate::pipeline::PipelineCallback) to bridge
/// the event-driven pipeline coordinator to the rest of the database (stream
/// executor, sinks, watermarks, checkpoints, table sources).
pub(crate) struct ConnectorPipelineCallback {
    pub(crate) executor: crate::stream_executor::StreamExecutor,
    pub(crate) stream_sources: Vec<(String, streaming::Source<crate::catalog::ArrowRecord>)>,
    #[allow(clippy::type_complexity)]
    pub(crate) sinks: Vec<(
        String,
        crate::sink_task::SinkTaskHandle,
        Option<String>,
        String, // input stream name (FROM clause target)
    )>,
    pub(crate) watermark_states: FxHashMap<String, SourceWatermarkState>,
    pub(crate) source_entries_for_wm: FxHashMap<String, Arc<crate::catalog::SourceEntry>>,
    pub(crate) source_ids: FxHashMap<String, usize>,
    pub(crate) tracker: Option<laminar_core::time::WatermarkTracker>,
    pub(crate) counters: Arc<crate::metrics::PipelineCounters>,
    pub(crate) pipeline_watermark: Arc<std::sync::atomic::AtomicI64>,
    pub(crate) checkpoint_in_progress: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    #[allow(clippy::type_complexity)]
    pub(crate) table_sources: Vec<(
        String,
        Box<dyn laminar_connectors::reference::ReferenceTableSource>,
        laminar_connectors::reference::RefreshMode,
    )>,
    pub(crate) table_store: Arc<parking_lot::RwLock<crate::table_store::TableStore>>,
    pub(crate) lookup_registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    /// Cached `SessionContext` for sink WHERE filters (avoids per-batch allocation).
    pub(crate) filter_ctx: SessionContext,
    /// Lazily-compiled sink filter expressions, indexed by sink position.
    pub(crate) compiled_sink_filters: Vec<Option<Arc<dyn PhysicalExpr>>>,
    pub(crate) last_checkpoint: std::time::Instant,
    /// `None` = no automatic checkpointing (manual only via coordinator).
    pub(crate) checkpoint_interval: Option<std::time::Duration>,
    pub(crate) pipeline_hash: Option<u64>,
    pub(crate) delivery_guarantee: laminar_connectors::connector::DeliveryGuarantee,
    /// DAG executor for lowered queries (`None` if no queries were lowered or DAG lowering is disabled).
    /// Used by Phase 7b when `use_dag_lowering` is enabled.
    pub(crate) dag_executor: Option<laminar_core::dag::DagExecutor>,
    /// Query names that were successfully lowered to the DAG (used in checkpoint path).
    #[allow(dead_code)]
    pub(crate) dag_query_names: rustc_hash::FxHashSet<Arc<str>>,
    /// DAG source node IDs per source name.
    pub(crate) dag_source_node_ids: FxHashMap<Arc<str>, laminar_core::dag::NodeId>,
    /// DAG sink node ID → query name mapping (for output routing).
    pub(crate) dag_sink_to_query: FxHashMap<laminar_core::dag::NodeId, Arc<str>>,
    /// Pre-aggregation SQL per DAG-handled query name (reserved for future pre-agg routing).
    #[allow(dead_code)]
    pub(crate) dag_pre_agg_sql: FxHashMap<Arc<str>, String>,
    /// Whether DAG lowering is active for this pipeline.
    pub(crate) use_dag_lowering: bool,
    /// Cycle duration histogram for percentile tracking (single-threaded access).
    pub(crate) cycle_histogram: std::cell::RefCell<crate::checkpoint_coordinator::DurationHistogram>,
}

impl ConnectorPipelineCallback {
    /// Try to compile sink filter SQL to `PhysicalExpr` for sinks that haven't been compiled yet.
    async fn compile_pending_sink_filters(
        &mut self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) {
        // Ensure the compiled_sink_filters vec has the right length.
        while self.compiled_sink_filters.len() < self.sinks.len() {
            self.compiled_sink_filters.push(None);
        }

        for (i, (_, _, filter_sql, sink_input)) in self.sinks.iter().enumerate() {
            // Skip if no filter or already compiled.
            if filter_sql.is_none() || self.compiled_sink_filters[i].is_some() {
                continue;
            }
            // Need a batch to determine the schema.
            let Some(batches) = results.get(sink_input.as_str()) else {
                continue;
            };
            let Some(batch) = batches.first() else {
                continue;
            };
            let schema = batch.schema();
            if let Some(compiled) =
                compile_sink_filter_sql(&self.filter_ctx, filter_sql.as_deref().unwrap(), &schema)
                    .await
            {
                self.compiled_sink_filters[i] = Some(compiled);
            }
        }
    }
}

#[async_trait::async_trait]
#[allow(clippy::too_many_lines)]
impl crate::pipeline::PipelineCallback for ConnectorPipelineCallback {
    async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        watermark: i64,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, String> {
        // StreamExecutor processes non-DAG queries (DAG queries are skipped).
        let mut results = self
            .executor
            .execute_cycle(source_batches, watermark)
            .await
            .map_err(|e| format!("{e}"))?;

        // Route lowered queries through the DAG executor when enabled.
        if self.use_dag_lowering {
            if let Some(dag) = &mut self.dag_executor {
                // Feed source batches to DAG source nodes.
                for (source_name, batches) in source_batches {
                    if let Some(&node_id) = self.dag_source_node_ids.get(source_name) {
                        for batch in batches {
                            if batch.num_rows() > 0 {
                                let event =
                                    laminar_core::operator::Event::new(watermark, batch.clone());
                                if let Err(e) = dag.process_event(node_id, event) {
                                    tracing::warn!(
                                        source = %source_name,
                                        error = %e,
                                        "DAG process_event failed"
                                    );
                                }
                            }
                        }
                    }
                }

                // Propagate watermark to all DAG source nodes.
                for &node_id in self.dag_source_node_ids.values() {
                    if let Err(e) = dag.process_watermark(node_id, watermark) {
                        tracing::debug!(error = %e, "DAG watermark propagation failed");
                    }
                }

                // Collect DAG sink outputs and merge into results.
                let dag_outputs = dag.take_all_sink_outputs();
                for (sink_id, events) in dag_outputs {
                    if let Some(query_name) = self.dag_sink_to_query.get(&sink_id) {
                        let batches: Vec<RecordBatch> = events
                            .into_iter()
                            .filter(|e| e.data.num_rows() > 0)
                            .map(|e| (*e.data).clone())
                            .collect();
                        if !batches.is_empty() {
                            results.insert(Arc::clone(query_name), batches);
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    fn push_to_streams(&self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        for (stream_name, src) in &self.stream_sources {
            if let Some(batches) = results.get(stream_name.as_str()) {
                for batch in batches {
                    if batch.num_rows() > 0 {
                        #[allow(clippy::cast_possible_truncation)]
                        let row_count = batch.num_rows() as u64;
                        self.counters
                            .events_emitted
                            .fetch_add(row_count, std::sync::atomic::Ordering::Relaxed);
                        if src.push_arrow(batch.clone()).is_err() {
                            #[allow(clippy::cast_possible_truncation)]
                            let dropped = batch.num_rows() as u64;
                            self.counters
                                .events_dropped
                                .fetch_add(dropped, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }

    async fn write_to_sinks(&mut self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        // Lazy-compile any pending sink filters.
        self.compile_pending_sink_filters(results).await;

        // Route results to sinks concurrently, filtered by FROM clause.
        let filter_ctx = self.filter_ctx.clone(); // Arc bump — cheap
        let sink_futures: Vec<_> = self
            .sinks
            .iter()
            .enumerate()
            .filter_map(|(sink_idx, (sink_name, handle, filter_expr, sink_input))| {
                // Route by FROM clause: only send matching results.
                let batches = results.get(sink_input.as_str())?;
                if batches.is_empty() {
                    return None;
                }
                let sink_name = sink_name.clone();
                let handle = handle.clone();
                let compiled_filter = self
                    .compiled_sink_filters
                    .get(sink_idx)
                    .and_then(Clone::clone);
                let filter_expr = filter_expr.clone();
                let batches = batches.clone();
                let ctx = filter_ctx.clone();
                Some(async move {
                    for batch in &batches {
                        let filtered = if let Some(ref phys) = compiled_filter {
                            // Use compiled PhysicalExpr — no SQL overhead
                            match apply_compiled_sink_filter(batch, phys) {
                                Ok(Some(fb)) => fb,
                                Ok(None) => continue,
                                Err(e) => {
                                    tracing::warn!(
                                        sink = %sink_name,
                                        error = %e,
                                        "Compiled sink filter error"
                                    );
                                    continue;
                                }
                            }
                        } else if let Some(ref filter_sql) = filter_expr {
                            // Fallback to SQL-based filter
                            match apply_filter(&ctx, batch, filter_sql).await {
                                Ok(Some(fb)) => fb,
                                Ok(None) => continue,
                                Err(e) => {
                                    tracing::warn!(
                                        sink = %sink_name,
                                        filter = %filter_sql,
                                        error = %e,
                                        "Sink filter error"
                                    );
                                    continue;
                                }
                            }
                        } else {
                            batch.clone()
                        };

                        if filtered.num_rows() > 0 {
                            if let Err(e) = handle.write_batch(filtered).await {
                                tracing::warn!(
                                    sink = %sink_name,
                                    error = %e,
                                    "Sink write error"
                                );
                            }
                        }
                    }
                })
            })
            .collect();
        futures::future::join_all(sink_futures).await;
    }

    fn extract_watermark(&mut self, source_name: &str, batch: &RecordBatch) {
        if let Some(wm_state) = self.watermark_states.get_mut(source_name) {
            // Check external watermarks from Source::watermark() calls.
            if let Some(entry) = self.source_entries_for_wm.get(source_name) {
                let external_wm = entry.source.current_watermark();
                if let Some(wm) = wm_state.generator.advance_watermark(external_wm) {
                    if let Some(ref mut trk) = self.tracker {
                        if let Some(sid) = self.source_ids.get(source_name) {
                            if let Some(global_wm) = trk.update_source(*sid, wm.timestamp()) {
                                self.pipeline_watermark.store(
                                    global_wm.timestamp(),
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            }

            // Extract watermark from batch data.
            if let Ok(max_ts) = wm_state.extractor.extract(batch) {
                if let Some(wm) = wm_state.generator.on_event(max_ts) {
                    if let Some(entry) = self.source_entries_for_wm.get(source_name) {
                        entry.source.watermark(wm.timestamp());
                    }
                    if let Some(ref mut trk) = self.tracker {
                        if let Some(sid) = self.source_ids.get(source_name) {
                            if let Some(global_wm) = trk.update_source(*sid, wm.timestamp()) {
                                self.pipeline_watermark.store(
                                    global_wm.timestamp(),
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            }
        }

        // Update ingestion counters.
        #[allow(clippy::cast_possible_truncation)]
        let row_count = batch.num_rows() as u64;
        self.counters
            .events_ingested
            .fetch_add(row_count, std::sync::atomic::Ordering::Relaxed);
        self.counters
            .total_batches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn filter_late_rows(&self, source_name: &str, batch: &RecordBatch) -> Option<RecordBatch> {
        if let Some(wm_state) = self.watermark_states.get(source_name) {
            let current_wm = wm_state.generator.current_watermark();
            if current_wm > i64::MIN {
                return filter_late_rows(batch, &wm_state.column, current_wm, wm_state.format);
            }
        }
        // No watermark configured → pass through all rows.
        Some(batch.clone())
    }

    fn current_watermark(&self) -> i64 {
        self.pipeline_watermark
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Timer-based checkpoint (at-least-once semantics).
    ///
    /// Source offsets are captured BEFORE operator state. On recovery:
    /// consumer replays from offset → operator may re-process some
    /// records. This is correct for at-least-once but NOT exactly-once.
    /// For exactly-once, use barrier-aligned checkpoints instead.
    async fn maybe_checkpoint(
        &mut self,
        force: bool,
        source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> bool {
        use crate::checkpoint_coordinator::source_to_connector_checkpoint;

        // Under exactly-once, only barrier-aligned checkpoints are consistent.
        // Timer-based checkpoints are skipped (barrier path handles exactly-once).
        if !force
            && self.delivery_guarantee
                == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce
        {
            tracing::debug!("skipping timer checkpoint under exactly-once (use barriers)");
            return false;
        }

        if self
            .counters
            .cycles
            .load(std::sync::atomic::Ordering::Relaxed)
            == 0
        {
            return false;
        }

        if !force
            && self
                .checkpoint_in_progress
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            return false;
        }

        if !force {
            let Some(interval) = self.checkpoint_interval else {
                return false; // no auto-checkpointing configured
            };
            if self.last_checkpoint.elapsed() < interval {
                return false;
            }
        }

        if force {
            // Wait for any in-flight checkpoint to complete.
            while self
                .checkpoint_in_progress
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        // Capture table source offsets.
        let mut extra_tables = HashMap::with_capacity(self.table_sources.len());
        for (name, source, _) in &self.table_sources {
            extra_tables.insert(
                name.clone(),
                source_to_connector_checkpoint(&source.checkpoint()),
            );
        }

        // Convert source offsets from connector format to manifest format.
        let source_overrides: HashMap<String, _> = source_offsets
            .iter()
            .map(|(name, cp)| (name.clone(), source_to_connector_checkpoint(cp)))
            .collect();

        // Capture stream executor aggregate state: snapshot (sync) + serialize.
        let mut operator_states = HashMap::with_capacity(1);
        match self.executor.snapshot_state() {
            Ok(Some(cp)) => {
                match crate::stream_executor::StreamExecutor::serialize_checkpoint(&cp) {
                    Ok(bytes) => {
                        operator_states.insert("stream_executor".to_string(), bytes);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Stream executor checkpoint serialization failed");
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(error = %e, "Stream executor checkpoint capture failed");
            }
        }

        // Collect per-source watermarks from the watermark tracker.
        let mut per_source_watermarks = HashMap::with_capacity(self.watermark_states.len());
        for (name, wm_state) in &self.watermark_states {
            let wm = wm_state.generator.current_watermark();
            if wm > i64::MIN {
                per_source_watermarks.insert(name.clone(), wm);
            }
        }

        if force {
            // Blocking checkpoint at shutdown.
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                match coord
                    .checkpoint_with_offsets(
                        operator_states,
                        None,
                        None,
                        extra_tables,
                        per_source_watermarks,
                        self.pipeline_hash,
                        source_overrides,
                    )
                    .await
                {
                    Ok(result) if result.success => {
                        tracing::info!(epoch = result.epoch, "Final pipeline checkpoint saved");
                    }
                    Ok(result) => {
                        tracing::warn!(
                            epoch = result.epoch,
                            error = ?result.error,
                            "Final checkpoint failed"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Final checkpoint error");
                    }
                }
            }
        } else {
            // Non-blocking periodic checkpoint.
            let coord_clone = Arc::clone(&self.coordinator);
            let in_progress = Arc::clone(&self.checkpoint_in_progress);
            in_progress.store(true, std::sync::atomic::Ordering::Relaxed);
            let pipeline_hash = self.pipeline_hash;

            tokio::spawn(async move {
                let mut guard = coord_clone.lock().await;
                if let Some(ref mut coord) = *guard {
                    match coord
                        .checkpoint_with_offsets(
                            operator_states,
                            None,
                            None,
                            extra_tables,
                            per_source_watermarks,
                            pipeline_hash,
                            source_overrides,
                        )
                        .await
                    {
                        Ok(result) if result.success => {
                            tracing::info!(
                                epoch = result.epoch,
                                duration_ms = result.duration.as_millis(),
                                "Pipeline checkpoint completed"
                            );
                        }
                        Ok(result) => {
                            tracing::warn!(
                                epoch = result.epoch,
                                error = ?result.error,
                                "Pipeline checkpoint failed"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Checkpoint error");
                        }
                    }
                }
                in_progress.store(false, std::sync::atomic::Ordering::Relaxed);
            });
        }

        self.last_checkpoint = std::time::Instant::now();
        true
    }

    async fn checkpoint_with_barrier(
        &mut self,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
    ) -> bool {
        use crate::checkpoint_coordinator::source_to_connector_checkpoint;

        if self
            .counters
            .cycles
            .load(std::sync::atomic::Ordering::Relaxed)
            == 0
        {
            return false;
        }

        // Capture table source offsets.
        let mut extra_tables = HashMap::with_capacity(self.table_sources.len());
        for (name, source, _) in &self.table_sources {
            extra_tables.insert(
                name.clone(),
                source_to_connector_checkpoint(&source.checkpoint()),
            );
        }

        // Capture stream executor aggregate state — now consistent because
        // all pre-barrier data has been executed.
        let mut operator_states = HashMap::with_capacity(1);
        match self.executor.snapshot_state() {
            Ok(Some(cp)) => {
                match crate::stream_executor::StreamExecutor::serialize_checkpoint(&cp) {
                    Ok(bytes) => {
                        operator_states.insert("stream_executor".to_string(), bytes);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Stream executor checkpoint serialization failed");
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(error = %e, "Stream executor checkpoint capture failed");
            }
        }

        // Collect per-source watermarks.
        let mut per_source_watermarks = HashMap::with_capacity(self.watermark_states.len());
        for (name, wm_state) in &self.watermark_states {
            let wm = wm_state.generator.current_watermark();
            if wm > i64::MIN {
                per_source_watermarks.insert(name.clone(), wm);
            }
        }

        // Barrier-captured source offsets go into source_offset_overrides
        // so they land in manifest.source_offsets (not table_offsets).
        // These positions are consistent with the operator state at the
        // barrier point and must not be re-queried from the live connectors.
        let mut source_overrides = HashMap::with_capacity(source_checkpoints.len());
        for (name, cp) in &source_checkpoints {
            source_overrides.insert(name.clone(), source_to_connector_checkpoint(cp));
        }

        let mut guard = self.coordinator.lock().await;
        if let Some(ref mut coord) = *guard {
            match coord
                .checkpoint_with_offsets(
                    operator_states,
                    None,
                    None,
                    extra_tables,
                    per_source_watermarks,
                    self.pipeline_hash,
                    source_overrides,
                )
                .await
            {
                Ok(result) if result.success => {
                    tracing::info!(
                        epoch = result.epoch,
                        duration_ms = result.duration.as_millis(),
                        "Barrier-aligned checkpoint completed"
                    );
                    self.last_checkpoint = std::time::Instant::now();
                    return true;
                }
                Ok(result) => {
                    tracing::warn!(
                        epoch = result.epoch,
                        error = ?result.error,
                        "Barrier-aligned checkpoint failed"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Barrier-aligned checkpoint error");
                }
            }
        }

        false
    }

    fn record_cycle(&self, events_ingested: u64, _batches: u64, elapsed_ns: u64) {
        let _ = events_ingested; // already recorded in extract_watermark
        self.counters
            .cycles
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.counters
            .last_cycle_duration_ns
            .store(elapsed_ns, std::sync::atomic::Ordering::Relaxed);

        // Record to histogram for percentile tracking.
        self.cycle_histogram
            .borrow_mut()
            .record(std::time::Duration::from_nanos(elapsed_ns));

        // Update percentile atomics every 100 cycles to amortize sort cost.
        let cycle_count = self
            .counters
            .cycles
            .load(std::sync::atomic::Ordering::Relaxed);
        if cycle_count.is_multiple_of(100) {
            let (p50, p95, p99) = self.cycle_histogram.borrow().percentiles();
            // DurationHistogram records in milliseconds; convert to nanoseconds.
            self.counters
                .cycle_p50_ns
                .store(p50 * 1_000_000, std::sync::atomic::Ordering::Relaxed);
            self.counters
                .cycle_p95_ns
                .store(p95 * 1_000_000, std::sync::atomic::Ordering::Relaxed);
            self.counters
                .cycle_p99_ns
                .store(p99 * 1_000_000, std::sync::atomic::Ordering::Relaxed);
        }
    }

    async fn poll_tables(&mut self) {
        use laminar_connectors::reference::RefreshMode;

        for (name, source, mode) in &mut self.table_sources {
            if matches!(mode, RefreshMode::SnapshotOnly | RefreshMode::Manual) {
                continue;
            }
            match source.poll_changes().await {
                Ok(Some(batch)) => {
                    // Single registry lookup — dispatch by variant.
                    let entry = self.lookup_registry.get_entry(name);
                    if let Some(
                        laminar_sql::datafusion::lookup_join_exec::RegisteredLookup::Partial(
                            partial,
                        ),
                    ) = &entry
                    {
                        update_partial_cache_from_batch(partial, &batch);
                        let mut ts = self.table_store.write();
                        if let Err(e) = ts.upsert_and_rebuild(name, &batch) {
                            tracing::warn!(table=%name, error=%e, "Table upsert error (partial)");
                        }
                    } else if let Some(
                        laminar_sql::datafusion::lookup_join_exec::RegisteredLookup::Versioned(
                            versioned,
                        ),
                    ) = &entry
                    {
                        // Versioned path: append new CDC rows, preserving
                        // all versions for temporal point-in-time lookups.
                        let combined = if versioned.batch.num_rows() == 0
                            || versioned.batch.schema().fields().is_empty()
                        {
                            batch.clone()
                        } else {
                            match arrow::compute::concat_batches(
                                &versioned.batch.schema(),
                                [&versioned.batch, &batch],
                            ) {
                                Ok(b) => b,
                                Err(e) => {
                                    tracing::warn!(
                                        table=%name, error=%e,
                                        "Versioned table concat error (schema mismatch?); \
                                         keeping existing state"
                                    );
                                    // Preserve existing versioned history rather than
                                    // discarding it. The CDC batch is silently dropped.
                                    continue;
                                }
                            }
                        };
                        // Build versioned index from the combined batch.
                        let key_indices: Vec<usize> = versioned
                            .key_columns
                            .iter()
                            .filter_map(|k| combined.schema().index_of(k).ok())
                            .collect();
                        let Ok(version_col_idx) =
                            combined.schema().index_of(&versioned.version_column)
                        else {
                            tracing::warn!(
                                table=%name,
                                version_col=%versioned.version_column,
                                "Version column not found; skipping index rebuild"
                            );
                            continue;
                        };
                        let index =
                            match laminar_sql::datafusion::lookup_join_exec::VersionedIndex::build(
                                &combined,
                                &key_indices,
                                version_col_idx,
                            ) {
                                Ok(idx) => Arc::new(idx),
                                Err(e) => {
                                    tracing::warn!(
                                        table=%name, error=%e,
                                        "Versioned index build error"
                                    );
                                    continue;
                                }
                            };
                        self.lookup_registry.register_versioned(
                            name,
                            laminar_sql::datafusion::VersionedLookupState {
                                batch: combined,
                                index,
                                key_columns: versioned.key_columns.clone(),
                                version_column: versioned.version_column.clone(),
                                stream_time_column: versioned.stream_time_column.clone(),
                            },
                        );
                        let mut ts = self.table_store.write();
                        if let Err(e) = ts.upsert_and_rebuild(name, &batch) {
                            tracing::warn!(table=%name, error=%e, "Table upsert error (versioned)");
                        }
                    } else {
                        let maybe_batch = {
                            let mut ts = self.table_store.write();
                            if let Err(e) = ts.upsert_and_rebuild(name, &batch) {
                                tracing::warn!(table=%name, error=%e, "Table upsert error");
                                None
                            } else if ts.is_persistent(name) {
                                None
                            } else {
                                ts.to_record_batch(name)
                            }
                        };
                        // Update lookup registry for join operators.
                        // DataFusion table registration is NOT needed here — all
                        // tables use ReferenceTableProvider which reads live data.
                        if let Some(rb) = maybe_batch {
                            self.lookup_registry.register(
                                name,
                                laminar_sql::datafusion::LookupSnapshot {
                                    batch: rb,
                                    key_columns: vec![],
                                },
                            );
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(table=%name, error=%e, "Table poll error");
                }
            }
        }
    }
}

/// Update a partial foyer cache from a CDC batch by inserting or deleting
/// each row keyed by the primary key column(s).
///
/// CDC delete detection: if the batch has a column named `__op`, `__operation`,
/// or `op` with values `"d"`, `"D"`, `"delete"`, or `"DELETE"`, the row is
/// removed from the cache instead of upserted.
fn update_partial_cache_from_batch(
    partial: &laminar_sql::datafusion::PartialLookupState,
    batch: &RecordBatch,
) {
    use arrow_array::{Array, StringArray};

    if partial.key_columns.is_empty() {
        return;
    }

    let key_cols: Vec<_> = partial
        .key_columns
        .iter()
        .filter_map(|name| {
            batch
                .schema()
                .index_of(name)
                .ok()
                .map(|idx| batch.column(idx).clone())
        })
        .collect();
    if key_cols.len() != partial.key_columns.len() {
        return;
    }

    let Ok(converter) = arrow::row::RowConverter::new(partial.key_sort_fields.clone()) else {
        return;
    };
    let Ok(rows) = converter.convert_columns(&key_cols) else {
        return;
    };

    // Detect CDC operation column for delete handling.
    let op_col_idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| matches!(f.name().as_str(), "__op" | "__operation" | "op"));
    let op_array = op_col_idx.and_then(|idx| {
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| (idx, a))
    });

    let num_rows = batch.num_rows();
    for row in 0..num_rows {
        let key = rows.row(row);

        let is_delete = op_array.is_some_and(|(_, arr)| {
            !arr.is_null(row) && matches!(arr.value(row), "d" | "D" | "delete" | "DELETE")
        });

        if is_delete {
            partial.foyer_cache.invalidate(key.as_ref());
        } else {
            let row_batch = batch.slice(row, 1);
            partial.foyer_cache.insert(key.as_ref(), row_batch);
        }
    }
}

/// Encode an Arrow schema as a compact string for passing through `ConnectorConfig`.
///
/// Format: `name:type,name:type,...` where type is the Arrow `DataType` debug name.
/// Example: `symbol:Utf8,price:Float64,volume:Int64`
pub(crate) fn encode_arrow_schema(schema: &arrow_schema::Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| format!("{}:{:?}", f.name(), f.data_type()))
        .collect::<Vec<_>>()
        .join(",")
}

/// Apply a SQL WHERE filter to a `RecordBatch` using a cached `SessionContext`.
///
/// Each call uses a unique temporary table name so concurrent calls on the
/// same context (via `join_all`) do not interfere with each other.
///
/// Returns `Ok(Some(filtered_batch))` if rows match, `Ok(None)` if no rows match,
/// or an error if the filter expression is invalid.
async fn apply_filter(
    ctx: &SessionContext,
    batch: &RecordBatch,
    filter_sql: &str,
) -> Result<Option<RecordBatch>, DbError> {
    // Unique table name per call to avoid concurrent conflicts.
    let id = FILTER_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let table_name = format!("{FILTER_INPUT_TABLE}_{id}");

    let schema = batch.schema();

    // Register the batch as a temporary table
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch.clone()]])
        .map_err(|e| DbError::query_pipeline("sink filter", &e))?;

    ctx.register_table(&*table_name, Arc::new(mem_table))
        .map_err(|e| DbError::query_pipeline("sink filter", &e))?;

    // Execute the filter query, always deregistering the temp table afterward.
    let sql = format!("SELECT * FROM {table_name} WHERE {filter_sql}");
    let result = async {
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| DbError::query_pipeline("sink filter", &e))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| DbError::query_pipeline("sink filter", &e))?;

        if batches.is_empty() {
            return Ok(None);
        }

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        if total_rows == 0 {
            return Ok(None);
        }

        // Merge all result batches into one (DataFusion may split output across
        // multiple partitions, so dropping all but the first would lose rows).
        if batches.len() == 1 {
            return Ok(batches.into_iter().next());
        }
        let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .map_err(|e| DbError::query_pipeline_arrow("sink filter concat", &e))?;
        Ok(Some(merged))
    }
    .await;

    // Clean up: deregister the temporary table to avoid catalog bloat.
    let _ = ctx.deregister_table(&*table_name);

    result
}

/// Apply a compiled `PhysicalExpr` filter to a batch.
fn apply_compiled_sink_filter(
    batch: &RecordBatch,
    filter: &Arc<dyn PhysicalExpr>,
) -> Result<Option<RecordBatch>, DbError> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let result = filter
        .evaluate(batch)
        .map_err(|e| DbError::Pipeline(format!("sink filter evaluate: {e}")))?;
    let mask = result
        .into_array(batch.num_rows())
        .map_err(|e| DbError::Pipeline(format!("sink filter to array: {e}")))?;
    let bool_arr = mask
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .ok_or_else(|| DbError::Pipeline("sink filter not boolean".into()))?;
    let filtered = arrow::compute::filter_record_batch(batch, bool_arr)
        .map_err(|e| DbError::Pipeline(format!("sink filter: {e}")))?;
    if filtered.num_rows() == 0 {
        Ok(None)
    } else {
        Ok(Some(filtered))
    }
}

/// Try to compile a sink filter SQL expression to a `PhysicalExpr`.
///
/// Plans the filter as a full SQL query against an empty table with the batch
/// schema, then extracts the Filter predicate from the logical plan and
/// compiles it. Returns `None` if compilation fails (caller falls back to
/// SQL-based filter).
async fn compile_sink_filter_sql(
    ctx: &SessionContext,
    filter_sql: &str,
    batch_schema: &SchemaRef,
) -> Option<Arc<dyn PhysicalExpr>> {
    let table_name = "__compile_filter";
    let empty =
        datafusion::datasource::MemTable::try_new(batch_schema.clone(), vec![vec![]]).ok()?;
    let _ = ctx.deregister_table(table_name);
    ctx.register_table(table_name, Arc::new(empty)).ok()?;

    let sql = format!("SELECT * FROM {table_name} WHERE {filter_sql}");
    let plan = {
        let df = ctx.sql(&sql).await.ok()?;
        df.logical_plan().clone()
    };
    let _ = ctx.deregister_table(table_name);

    // Walk plan to find Filter node
    let filter_expr = find_filter_predicate(&plan)?;

    // Compile against the batch schema
    let df_schema = DFSchema::try_from(batch_schema.as_ref().clone()).ok()?;
    let state = ctx.state();
    let props = state.execution_props();
    create_physical_expr(&filter_expr, &df_schema, props).ok()
}

/// Walk a logical plan to find the first Filter predicate.
fn find_filter_predicate(plan: &datafusion_expr::LogicalPlan) -> Option<datafusion_expr::Expr> {
    match plan {
        datafusion_expr::LogicalPlan::Filter(f) => Some(f.predicate.clone()),
        datafusion_expr::LogicalPlan::Projection(p) => find_filter_predicate(&p.input),
        datafusion_expr::LogicalPlan::Sort(s) => find_filter_predicate(&s.input),
        datafusion_expr::LogicalPlan::Limit(l) => find_filter_predicate(&l.input),
        _ => {
            for input in plan.inputs() {
                if let Some(expr) = find_filter_predicate(input) {
                    return Some(expr);
                }
            }
            None
        }
    }
}
