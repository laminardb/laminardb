//! `PipelineCallback` implementation bridging the event-driven pipeline
//! coordinator to the stream executor, sinks, watermarks, checkpoints,
//! and table sources.
#![allow(clippy::disallowed_types)] // cold path

use std::sync::Arc;

use std::collections::HashMap;

use arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_core::streaming;
use rustc_hash::FxHashMap;

use crate::db::{filter_late_rows, SourceWatermarkState};
use crate::error::DbError;

/// Internal table name used by the sink WHERE filter.
const FILTER_INPUT_TABLE: &str = "__laminar_filter_input";

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
    pub(crate) table_store: Arc<parking_lot::Mutex<crate::table_store::TableStore>>,
    pub(crate) ctx: SessionContext,
    pub(crate) last_checkpoint: std::time::Instant,
    pub(crate) pipeline_hash: Option<u64>,
}

#[async_trait::async_trait]
#[allow(clippy::too_many_lines)]
impl crate::pipeline::PipelineCallback for ConnectorPipelineCallback {
    async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<String, Vec<RecordBatch>>,
        watermark: i64,
    ) -> Result<FxHashMap<String, Vec<RecordBatch>>, String> {
        self.executor
            .execute_cycle(source_batches, watermark)
            .await
            .map_err(|e| format!("{e}"))
    }

    fn push_to_streams(&self, results: &FxHashMap<String, Vec<RecordBatch>>) {
        for (stream_name, src) in &self.stream_sources {
            if let Some(batches) = results.get(stream_name) {
                for batch in batches {
                    if batch.num_rows() > 0 {
                        #[allow(clippy::cast_possible_truncation)]
                        let row_count = batch.num_rows() as u64;
                        self.counters
                            .events_emitted
                            .fetch_add(row_count, std::sync::atomic::Ordering::Relaxed);
                        let _ = src.push_arrow(batch.clone());
                    }
                }
            }
        }
    }

    async fn write_to_sinks(&mut self, results: &FxHashMap<String, Vec<RecordBatch>>) {
        // Route results to sinks concurrently, filtered by FROM clause.
        let sink_futures: Vec<_> = self
            .sinks
            .iter()
            .filter_map(|(sink_name, handle, filter_expr, sink_input)| {
                // Route by FROM clause: only send matching results.
                let batches = results.get(sink_input.as_str())?;
                if batches.is_empty() {
                    return None;
                }
                let sink_name = sink_name.clone();
                let handle = handle.clone();
                let filter_expr = filter_expr.clone();
                let batches = batches.clone();
                Some(async move {
                    for batch in &batches {
                        let filtered = if let Some(ref filter_sql) = filter_expr {
                            match apply_filter(batch, filter_sql).await {
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

    async fn maybe_checkpoint(&mut self, force: bool) -> bool {
        use crate::checkpoint_coordinator::source_to_connector_checkpoint;

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

        // For periodic checkpoints, check the timer.
        if !force {
            // No checkpoint_interval configured → coordinator handles this.
            if self.last_checkpoint.elapsed() < std::time::Duration::from_millis(1000) {
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

        // Capture stream executor aggregate state.
        let mut operator_states = HashMap::with_capacity(1);
        match self.executor.checkpoint_state() {
            Ok(Some(bytes)) => {
                operator_states.insert("stream_executor".to_string(), bytes);
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
                    .checkpoint_with_extra_tables(
                        operator_states,
                        None,
                        0,
                        Vec::new(),
                        None,
                        extra_tables,
                        per_source_watermarks,
                        self.pipeline_hash,
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
                        .checkpoint_with_extra_tables(
                            operator_states,
                            None,
                            0,
                            Vec::new(),
                            None,
                            extra_tables,
                            per_source_watermarks,
                            pipeline_hash,
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
        match self.executor.checkpoint_state() {
            Ok(Some(bytes)) => {
                operator_states.insert("stream_executor".to_string(), bytes);
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

        // Merge barrier-captured source offsets into the extra_tables map.
        for (name, cp) in &source_checkpoints {
            extra_tables
                .entry(name.clone())
                .or_insert_with(|| source_to_connector_checkpoint(cp));
        }

        let mut guard = self.coordinator.lock().await;
        if let Some(ref mut coord) = *guard {
            match coord
                .checkpoint_with_extra_tables(
                    operator_states,
                    None,
                    0,
                    Vec::new(),
                    None,
                    extra_tables,
                    per_source_watermarks,
                    self.pipeline_hash,
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
    }

    async fn poll_tables(&mut self) {
        use laminar_connectors::reference::RefreshMode;

        for (name, source, mode) in &mut self.table_sources {
            if matches!(mode, RefreshMode::SnapshotOnly | RefreshMode::Manual) {
                continue;
            }
            match source.poll_changes().await {
                Ok(Some(batch)) => {
                    let maybe_batch = {
                        let mut ts = self.table_store.lock();
                        if let Err(e) = ts.upsert_and_rebuild(name, &batch) {
                            tracing::warn!(table=%name, error=%e, "Table upsert error");
                            None
                        } else if ts.is_persistent(name) {
                            None
                        } else {
                            ts.to_record_batch(name)
                        }
                    };
                    if let Some(rb) = maybe_batch {
                        let schema = rb.schema();
                        let _ = self.ctx.deregister_table(name.as_str());
                        let data = if rb.num_rows() > 0 {
                            vec![vec![rb]]
                        } else {
                            vec![vec![]]
                        };
                        if let Ok(mem_table) =
                            datafusion::datasource::MemTable::try_new(schema, data)
                        {
                            let _ = self.ctx.register_table(name.as_str(), Arc::new(mem_table));
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

/// Apply a SQL WHERE filter to a `RecordBatch`.
///
/// Returns `Ok(Some(filtered_batch))` if rows match, `Ok(None)` if no rows match,
/// or an error if the filter expression is invalid.
async fn apply_filter(
    batch: &RecordBatch,
    filter_sql: &str,
) -> Result<Option<RecordBatch>, DbError> {
    let ctx = laminar_sql::create_session_context();
    let schema = batch.schema();

    // Register the batch as a temporary table
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch.clone()]])
        .map_err(|e| DbError::query_pipeline("sink filter", &e))?;

    ctx.register_table(FILTER_INPUT_TABLE, Arc::new(mem_table))
        .map_err(|e| DbError::query_pipeline("sink filter", &e))?;

    // Execute the filter query
    let sql = format!("SELECT * FROM {FILTER_INPUT_TABLE} WHERE {filter_sql}");
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

    // Concatenate all result batches
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if total_rows == 0 {
        return Ok(None);
    }

    // Return the first batch (typically there's only one)
    Ok(Some(batches.into_iter().next().unwrap()))
}
