//! Production `PipelineCallback` bridging coordinator to sinks, checkpoints, and watermarks.
#![allow(clippy::disallowed_types)] // cold path

use std::sync::Arc;
use std::time::Duration;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_common::DFSchema;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_core::alloc::{PriorityClass, PriorityGuard};
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
///
/// `ConnectorPipelineCallback` runs on a dedicated single-threaded tokio runtime
/// (laminar-compute thread). Serialization and checkpoint I/O run inline because:
/// 1. `tokio::spawn` on `current_thread` has no parallelism benefit
/// 2. `spawn_blocking` on `current_thread` has no dedicated blocking pool
/// 3. The compute thread is dedicated — blocking is acceptable
pub(crate) struct ConnectorPipelineCallback {
    pub(crate) graph: crate::operator_graph::OperatorGraph,
    pub(crate) stream_sources: Vec<(String, streaming::Source<crate::catalog::ArrowRecord>)>,
    #[allow(clippy::type_complexity)]
    pub(crate) sinks: Vec<(
        String,
        crate::sink_task::SinkTaskHandle,
        Option<String>,
        String, // input stream name (FROM clause target)
        bool,   // changelog-capable sink
    )>,
    pub(crate) watermark_states: FxHashMap<String, SourceWatermarkState>,
    pub(crate) source_entries_for_wm: FxHashMap<String, Arc<crate::catalog::SourceEntry>>,
    pub(crate) source_ids: FxHashMap<String, usize>,
    pub(crate) tracker: Option<laminar_core::time::WatermarkTracker>,
    pub(crate) prom: Arc<crate::engine_metrics::EngineMetrics>,
    pub(crate) pipeline_watermark: Arc<std::sync::atomic::AtomicI64>,
    pub(crate) coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    #[allow(clippy::type_complexity)]
    pub(crate) table_sources: Vec<(
        String,
        Box<dyn laminar_connectors::reference::ReferenceTableSource>,
        laminar_connectors::reference::RefreshMode,
    )>,
    pub(crate) table_store: Arc<parking_lot::RwLock<crate::table_store::TableStore>>,
    pub(crate) mv_store: Arc<parking_lot::RwLock<crate::mv_store::MvStore>>,
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
    /// Maximum time to wait for operator state serialization.
    pub(crate) serialization_timeout: Duration,
    /// Drained once per cycle by `write_to_sinks` to update sink metrics.
    pub(crate) sink_event_rx: laminar_core::streaming::AsyncConsumer<crate::sink_task::SinkEvent>,
    /// Set when a sink write times out in this cycle. Suppresses the next
    /// checkpoint to preserve the replay window.
    pub(crate) sink_timed_out: bool,
    pub(crate) shutdown_signal: Arc<tokio::sync::Notify>,
    /// Cluster control facade. Present only when this instance was
    /// built with a controller. Used to gate the periodic checkpoint
    /// trigger on `is_leader()` and to run `follower_checkpoint` on
    /// non-leaders.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Highest epoch a non-leader has already prepared for. Prevents
    /// re-running `follower_checkpoint` on the same announcement.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) last_follower_epoch: Option<u64>,
    /// Inbound channel for `db.checkpoint()` requests. The db sends a
    /// oneshot sender here; on the next cycle the callback captures
    /// operator state (via `checkpoint_with_barrier`-style path) and
    /// replies on that sender. Without this, `db.checkpoint()` reaches
    /// the coordinator with an empty `CheckpointRequest` and the
    /// leader's manifest is useless for restart recovery.
    pub(crate) force_ckpt_rx: Option<crate::db::ForceCheckpointRx>,
}

impl ConnectorPipelineCallback {
    /// `BackpressureFail` raises `shutdown`; coordinator exits via its drain path.
    fn map_graph_error(err: &crate::error::DbError, shutdown: &tokio::sync::Notify) -> String {
        if let crate::error::DbError::BackpressureFail(msg) = err {
            tracing::error!(reason = %msg, "backpressure_policy=Fail tripped; halting pipeline");
            shutdown.notify_one();
        }
        format!("{err}")
    }

    /// Cap each source's tracker-reported watermark by the cluster-wide
    /// minimum, when one has been published. A `None` `cluster_wm`
    /// leaves the map untouched — blindly capping to MIN would freeze
    /// every downstream event-time decision and hang the pipeline.
    #[cfg(feature = "cluster-unstable")]
    fn cap_source_watermarks_by_cluster_min(
        source_wms: &mut FxHashMap<Arc<str>, i64>,
        cluster_wm: Option<i64>,
    ) {
        let Some(cluster_wm) = cluster_wm else { return };
        for wm in source_wms.values_mut() {
            if cluster_wm < *wm {
                *wm = cluster_wm;
            }
        }
    }

    /// Follower-side dispatch: poll the leader's announcement, and
    /// if a fresh PREPARE is visible, capture local state and run
    /// `CheckpointCoordinator::follower_checkpoint`. Returns `true`
    /// when a checkpoint was driven this tick.
    /// Driven by `db.checkpoint()` via the `force_ckpt_rx` channel.
    /// Captures operator state (plus table/watermark metadata) and
    /// commits the manifest through the coordinator, returning the
    /// full `CheckpointResult` for the caller. Mirrors
    /// `checkpoint_with_barrier` but without the barrier-alignment
    /// gate — the caller has chosen to take a consistent snapshot
    /// of whatever state is currently in the accumulators.
    async fn force_capture_and_checkpoint(
        &mut self,
    ) -> Result<crate::checkpoint_coordinator::CheckpointResult, DbError> {
        use crate::checkpoint_coordinator::source_to_connector_checkpoint;
        let _priority = PriorityGuard::enter(PriorityClass::BackgroundIo);

        let operator_states = self
            .capture_and_serialize_operator_state()
            .await
            .map_err(DbError::Checkpoint)?;

        let mut extra_tables = HashMap::with_capacity(self.table_sources.len());
        for (name, source, _) in &self.table_sources {
            extra_tables.insert(
                name.clone(),
                source_to_connector_checkpoint(&source.checkpoint()),
            );
        }

        let mut per_source_watermarks = HashMap::with_capacity(self.watermark_states.len());
        for (name, wm_state) in &self.watermark_states {
            let wm = wm_state.generator.current_watermark();
            if wm > i64::MIN {
                per_source_watermarks.insert(name.clone(), wm);
            }
        }

        let request = crate::checkpoint_coordinator::CheckpointRequest {
            operator_states,
            watermark: None,
            table_store_checkpoint_path: None,
            extra_table_offsets: extra_tables,
            source_watermarks: per_source_watermarks,
            pipeline_hash: self.pipeline_hash,
            source_offset_overrides: HashMap::new(),
        };

        let mut guard = self.coordinator.lock().await;
        let coord = guard.as_mut().ok_or_else(|| {
            DbError::Checkpoint(
                "coordinator not initialized when force_capture_and_checkpoint ran".into(),
            )
        })?;
        // Phase 1.3: seed leader-side local watermark so
        // `await_prepare_quorum` can fold it into the cluster-wide min.
        let wm = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        coord.set_local_watermark_ms(if wm == i64::MIN { None } else { Some(wm) });
        let result = coord.checkpoint_with_offsets(request).await?;
        if result.success {
            self.last_checkpoint = std::time::Instant::now();
        }
        Ok(result)
    }

    #[cfg(feature = "cluster-unstable")]
    async fn maybe_follower_checkpoint(
        &mut self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
        source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> Option<u64> {
        use crate::checkpoint_coordinator::source_to_connector_checkpoint;
        use laminar_core::cluster::control::Phase;

        let ann = match controller.observe_barrier().await {
            Ok(Some(a)) if a.phase == Phase::Prepare => a,
            _ => return None,
        };
        if self.last_follower_epoch.is_some_and(|e| e >= ann.epoch) {
            return None;
        }
        self.last_follower_epoch = Some(ann.epoch);

        let operator_states = match self.capture_and_serialize_operator_state().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "follower state capture failed — skipping");
                return None;
            }
        };
        let mut extra_tables = HashMap::with_capacity(self.table_sources.len());
        for (name, source, _) in &self.table_sources {
            extra_tables.insert(
                name.clone(),
                source_to_connector_checkpoint(&source.checkpoint()),
            );
        }
        let source_overrides: HashMap<String, _> = source_offsets
            .iter()
            .map(|(name, cp)| (name.clone(), source_to_connector_checkpoint(cp)))
            .collect();
        let mut per_source_watermarks = HashMap::with_capacity(self.watermark_states.len());
        for (name, wm_state) in &self.watermark_states {
            let wm = wm_state.generator.current_watermark();
            if wm > i64::MIN {
                per_source_watermarks.insert(name.clone(), wm);
            }
        }
        let request = crate::checkpoint_coordinator::CheckpointRequest {
            operator_states,
            watermark: None,
            table_store_checkpoint_path: None,
            extra_table_offsets: extra_tables,
            source_watermarks: per_source_watermarks,
            pipeline_hash: self.pipeline_hash,
            source_offset_overrides: source_overrides,
        };

        let mut guard = self.coordinator.lock().await;
        let Some(ref mut coord) = *guard else {
            return None;
        };
        // Phase 1.3: stamp this instance's current pipeline watermark
        // into the coordinator so the next `BarrierAck` carries it.
        // i64::MIN means unset; don't propagate — leader treats us as
        // non-blocking.
        let wm = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        coord.set_local_watermark_ms(if wm == i64::MIN { None } else { Some(wm) });
        let follower_epoch = ann.epoch;
        match coord
            .follower_checkpoint(request, ann, std::time::Duration::from_secs(30))
            .await
        {
            Ok(true) => {
                tracing::info!(epoch = follower_epoch, "follower checkpoint committed");
                self.last_checkpoint = std::time::Instant::now();
                Some(follower_epoch)
            }
            Ok(false) => {
                tracing::warn!("follower checkpoint aborted (leader signalled Abort)");
                None
            }
            Err(e) => {
                tracing::warn!(error = %e, "follower checkpoint errored");
                None
            }
        }
    }

    async fn capture_and_serialize_operator_state(
        &mut self,
    ) -> Result<std::collections::HashMap<String, bytes::Bytes>, String> {
        let mut operator_states = HashMap::with_capacity(2);
        let cp = match self.graph.snapshot_state() {
            Ok(Some(cp)) => cp,
            Ok(None) => return Ok(operator_states),
            Err(e) => return Err(format!("snapshot failed: {e}")),
        };
        // Offload CPU-bound serialization to blocking thread pool.
        let timeout = self.serialization_timeout;
        let bytes = tokio::time::timeout(
            timeout,
            tokio::task::spawn_blocking(move || {
                crate::operator_graph::OperatorGraph::serialize_checkpoint(&cp)
            }),
        )
        .await
        .map_err(|_| format!("[LDB-6017] operator state serialization timed out ({timeout:?})"))?
        .map_err(|e| format!("serialize join error: {e}"))?
        .map_err(|e| format!("serialize error: {e}"))?;
        operator_states.insert("operator_graph".to_string(), bytes::Bytes::from(bytes));

        // Serialize MV result store — each MV gets its own entry keyed "mv:{name}".
        let mv_states = self
            .mv_store
            .read()
            .checkpoint_states()
            .map_err(|e| format!("MV checkpoint failed: {e}"))?;
        operator_states.extend(mv_states);

        Ok(operator_states)
    }

    async fn compile_pending_sink_filters(
        &mut self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) {
        // Ensure the compiled_sink_filters vec has the right length.
        while self.compiled_sink_filters.len() < self.sinks.len() {
            self.compiled_sink_filters.push(None);
        }

        for (i, (_, _, filter_sql, sink_input, _)) in self.sinks.iter().enumerate() {
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
        #[cfg_attr(not(feature = "cluster-unstable"), allow(unused_mut))]
        let mut source_wms: FxHashMap<Arc<str>, i64> = if let Some(ref tracker) = self.tracker {
            self.source_ids
                .iter()
                .filter_map(|(name, &sid)| {
                    tracker
                        .source_watermark(sid)
                        .map(|wm| (Arc::from(name.as_str()), wm))
                })
                .collect()
        } else {
            FxHashMap::default()
        };

        #[cfg(feature = "cluster-unstable")]
        {
            let cluster_wm = self
                .cluster_controller
                .as_ref()
                .and_then(|cc| cc.cluster_min_watermark());
            Self::cap_source_watermarks_by_cluster_min(&mut source_wms, cluster_wm);
        }

        let swm_ref = if source_wms.is_empty() {
            None
        } else {
            Some(&source_wms)
        };
        self.graph
            .execute_cycle(source_batches, watermark, swm_ref)
            .await
            .map_err(|e| Self::map_graph_error(&e, &self.shutdown_signal))
    }

    fn push_to_streams(&self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        for (stream_name, src) in &self.stream_sources {
            if let Some(batches) = results.get(stream_name.as_str()) {
                for batch in batches {
                    if batch.num_rows() > 0 {
                        #[allow(clippy::cast_possible_truncation)]
                        let row_count = batch.num_rows() as u64;
                        self.prom.events_emitted.inc_by(row_count);
                        if src.push_arrow(batch.clone()).is_err() {
                            #[allow(clippy::cast_possible_truncation)]
                            let dropped = batch.num_rows() as u64;
                            self.prom.events_dropped.inc_by(dropped);
                        }
                    }
                }
            }
        }
    }

    fn update_mv_stores(&self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        if results.is_empty() {
            return;
        }
        let mut store = self.mv_store.write();
        let mut updates = 0u64;
        for (stream_name, batches) in results {
            if store.has_mv(stream_name) {
                for batch in batches {
                    if batch.num_rows() > 0 {
                        store.update(stream_name, batch);
                        updates += 1;
                    }
                }
            }
        }
        if updates > 0 {
            self.prom.mv_updates.inc_by(updates);
            #[allow(clippy::cast_possible_truncation)]
            let bytes = store.total_bytes() as u64;
            #[allow(clippy::cast_possible_wrap)]
            self.prom.mv_bytes_stored.set(bytes as i64);
        }
    }

    async fn write_to_sinks(&mut self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        // Lazy-compile any pending sink filters.
        self.compile_pending_sink_filters(results).await;

        // Route results to sinks concurrently, filtered by FROM clause.
        // The per-call I/O timeout is enforced inside the sink task; the
        // bounded command channel backpressures us naturally. Failures
        // and timeouts are reported out of band via SinkEvents drained
        // below after join_all.
        let filter_ctx = self.filter_ctx.clone(); // Arc bump — cheap
        let sink_futures: Vec<_> = self
            .sinks
            .iter()
            .enumerate()
            .filter_map(
                |(sink_idx, (sink_name, handle, filter_expr, sink_input, changelog_capable))| {
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

                            let filtered = crate::changelog_filter::prepare_for_sink(
                                &filtered,
                                *changelog_capable,
                            );

                            if filtered.num_rows() > 0 {
                                if let Err(e) = handle.write_batch(filtered).await {
                                    // ChannelClosed metric is incremented when
                                    // we drain the event channel below; here
                                    // we just log and stop sending to this sink.
                                    tracing::warn!(
                                        sink = %sink_name,
                                        error = %e,
                                        "Sink command channel closed"
                                    );
                                    break;
                                }
                            }
                        }
                    })
                },
            )
            .collect();
        futures::future::join_all(sink_futures).await;

        // Barrier: wait for every sink task to finish processing the
        // commands we just enqueued, so the drain below catches any
        // failures before maybe_checkpoint can advance source offsets.
        let sync_futures = self.sinks.iter().map(|(name, handle, _, _, _)| {
            let name = name.clone();
            let handle = handle.clone();
            async move {
                if let Err(e) = handle.sync().await {
                    tracing::warn!(sink = %name, error = %e, "sink sync barrier failed");
                }
            }
        });
        futures::future::join_all(sync_futures).await;

        // Drain SinkEvents emitted by sink tasks during this cycle.
        while let Ok(event) = self.sink_event_rx.try_recv() {
            tracing::debug!(?event, "sink event");
            match &event {
                crate::sink_task::SinkEvent::WriteError { .. } => {
                    self.prom.sink_write_failures.inc();
                }
                crate::sink_task::SinkEvent::WriteTimeout { .. } => {
                    self.sink_timed_out = true;
                    self.prom.sink_write_timeouts.inc();
                }
                crate::sink_task::SinkEvent::ChannelClosed { .. } => {
                    self.prom.sink_task_channel_closed.inc();
                }
            }
        }
    }

    fn extract_watermark(&mut self, source_name: &str, batch: &RecordBatch) {
        if let Some(wm_state) = self.watermark_states.get_mut(source_name) {
            // Check external watermarks from Source::watermark() calls.
            if let Some(entry) = self.source_entries_for_wm.get(source_name) {
                let external_wm = entry.source.current_watermark();
                if let Some(wm) = wm_state.generator.advance_watermark(external_wm) {
                    self.prom
                        .source_watermark_ms
                        .with_label_values(&[source_name])
                        .set(wm.timestamp());
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
                    self.prom
                        .source_watermark_ms
                        .with_label_values(&[source_name])
                        .set(wm.timestamp());
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
        self.prom.events_ingested.inc_by(row_count);
        self.prom.batches.inc();
    }

    fn filter_late_rows(&self, source_name: &str, batch: &RecordBatch) -> Option<RecordBatch> {
        if let Some(wm_state) = self.watermark_states.get(source_name) {
            let current_wm = wm_state.generator.current_watermark();
            if current_wm > i64::MIN {
                return filter_late_rows(batch, &wm_state.column, current_wm);
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
    ) -> Option<u64> {
        use crate::checkpoint_coordinator::source_to_connector_checkpoint;
        let _priority = PriorityGuard::enter(PriorityClass::BackgroundIo);

        // Drain any pending `db.checkpoint()` requests first. Each
        // request gets a capture of operator state (the same path the
        // periodic barrier-aligned checkpoint uses), committed through
        // the coordinator, and the full `CheckpointResult` returned on
        // the oneshot. Without this, `db.checkpoint()` reaches the
        // coordinator with an empty `CheckpointRequest` and the
        // manifest has no operator state — restart loses every
        // `IncrementalAggState` accumulator on the invoking node. See
        // Phase 1.1 in docs/plans/cluster-production-readiness.md.
        let mut force_reqs: Vec<
            tokio::sync::oneshot::Sender<
                Result<crate::checkpoint_coordinator::CheckpointResult, DbError>,
            >,
        > = Vec::new();
        if let Some(rx) = self.force_ckpt_rx.as_mut() {
            while let Ok(reply) = rx.try_recv() {
                force_reqs.push(reply);
            }
        }
        for reply_tx in force_reqs {
            let result = self.force_capture_and_checkpoint().await;
            let _ = reply_tx.send(result);
        }

        // Cluster mode: only the leader fires periodic checkpoints.
        // Followers mirror the leader's epoch via `follower_checkpoint`
        // on observed PREPARE announcements. Forced checkpoints
        // (shutdown) run on whatever instance is shutting down.
        #[cfg(feature = "cluster-unstable")]
        if !force {
            if let Some(cc) = self.cluster_controller.clone() {
                if !cc.is_leader() {
                    return self.maybe_follower_checkpoint(cc, source_offsets).await;
                }
            }
        }

        // Under exactly-once, only barrier-aligned checkpoints are consistent.
        // Timer-based checkpoints are skipped (barrier path handles exactly-once).
        if !force
            && self.delivery_guarantee
                == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce
        {
            tracing::debug!("skipping timer checkpoint under exactly-once (use barriers)");
            return None;
        }

        if self.prom.cycles.get() == 0 {
            return None;
        }

        // After a sink timeout, skip one checkpoint cycle so that source
        // offsets don't advance past the dropped batch. The NEXT cycle will
        // clear this flag and checkpoint normally. This preserves at-least-once:
        // on recovery, the source replays from the pre-drop checkpoint.
        if !force && self.sink_timed_out {
            self.sink_timed_out = false;
            tracing::info!("skipping checkpoint after sink timeout to preserve replay window");
            return None;
        }

        if !force {
            let Some(interval) = self.checkpoint_interval else {
                return None; // no auto-checkpointing configured
            };
            if self.last_checkpoint.elapsed() < interval {
                return None;
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

        // Capture stream executor aggregate state and serialize on blocking thread.
        // Abort the checkpoint if serialization fails — persisting source
        // offsets without matching operator state would cause data loss on
        // recovery (offsets advance past unreplayable state).
        let operator_states = match self.capture_and_serialize_operator_state().await {
            Ok(states) => states,
            Err(e) => {
                tracing::warn!(error = %e, "Stream executor checkpoint failed — skipping checkpoint");
                return None;
            }
        };

        // Collect per-source watermarks from the watermark tracker.
        let mut per_source_watermarks = HashMap::with_capacity(self.watermark_states.len());
        for (name, wm_state) in &self.watermark_states {
            let wm = wm_state.generator.current_watermark();
            if wm > i64::MIN {
                per_source_watermarks.insert(name.clone(), wm);
            }
        }

        let request = crate::checkpoint_coordinator::CheckpointRequest {
            operator_states,
            watermark: None,
            table_store_checkpoint_path: None,
            extra_table_offsets: extra_tables,
            source_watermarks: per_source_watermarks,
            pipeline_hash: self.pipeline_hash,
            source_offset_overrides: source_overrides,
        };

        let committed_epoch: Option<u64> = {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                match coord.checkpoint_with_offsets(request).await {
                    Ok(result) if result.success => {
                        if force {
                            tracing::info!(
                                epoch = result.epoch,
                                "Final pipeline checkpoint saved"
                            );
                        } else {
                            tracing::info!(
                                epoch = result.epoch,
                                duration_ms = result.duration.as_millis(),
                                "Pipeline checkpoint completed"
                            );
                        }
                        Some(result.epoch)
                    }
                    Ok(result) => {
                        tracing::warn!(
                            epoch = result.epoch,
                            error = ?result.error,
                            "Pipeline checkpoint failed"
                        );
                        None
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Checkpoint error");
                        None
                    }
                }
            } else {
                None
            }
        };

        self.last_checkpoint = std::time::Instant::now();
        committed_epoch
    }

    async fn checkpoint_with_barrier(
        &mut self,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
    ) -> Option<u64> {
        use crate::checkpoint_coordinator::source_to_connector_checkpoint;
        let _priority = PriorityGuard::enter(PriorityClass::BackgroundIo);

        if self.prom.cycles.get() == 0 {
            return None;
        }

        // Clear after one suppression — the timer-based path that also
        // clears this flag is unreachable when barrier checkpointing is active.
        if self.sink_timed_out {
            self.sink_timed_out = false;
            tracing::warn!(
                "skipping barrier checkpoint after sink timeout to preserve replay window"
            );
            return None;
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
        // all pre-barrier data has been executed. Serialize on blocking thread.
        // Abort if serialization fails — persisting source offsets without
        // matching operator state would cause data loss on recovery.
        let operator_states = match self.capture_and_serialize_operator_state().await {
            Ok(states) => states,
            Err(e) => {
                tracing::warn!(error = %e, "Stream executor barrier checkpoint failed — skipping");
                return None;
            }
        };

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
        let request = crate::checkpoint_coordinator::CheckpointRequest {
            operator_states,
            watermark: None,
            table_store_checkpoint_path: None,
            extra_table_offsets: extra_tables,
            source_watermarks: per_source_watermarks,
            pipeline_hash: self.pipeline_hash,
            source_offset_overrides: source_overrides,
        };

        if let Some(ref mut coord) = *guard {
            match coord.checkpoint_with_offsets(request).await {
                Ok(result) if result.success => {
                    tracing::info!(
                        epoch = result.epoch,
                        duration_ms = result.duration.as_millis(),
                        "Barrier-aligned checkpoint completed"
                    );
                    self.last_checkpoint = std::time::Instant::now();
                    return Some(result.epoch);
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

        None
    }

    fn record_cycle(&self, events_ingested: u64, _batches: u64, elapsed_ns: u64) {
        let _ = events_ingested; // already recorded in extract_watermark
        self.prom.cycles.inc();
        #[allow(clippy::cast_precision_loss)]
        self.prom
            .cycle_duration
            .observe(elapsed_ns as f64 / 1_000_000_000.0);
    }

    fn apply_control(&mut self, msg: crate::pipeline::ControlMsg) {
        match msg {
            crate::pipeline::ControlMsg::AddStream {
                name,
                sql,
                emit_clause,
                window_config,
                order_config,
            } => {
                self.graph.add_query(
                    name.clone(),
                    sql,
                    emit_clause,
                    window_config,
                    order_config,
                    None,
                );
                tracing::info!(stream = %name, "Stream added via control channel");
            }
            crate::pipeline::ControlMsg::DropStream { name } => {
                self.graph.remove_query(&name);
                tracing::info!(stream = %name, "Stream removed via control channel");
            }
            crate::pipeline::ControlMsg::AddSourceSchema { name, schema } => {
                self.graph.register_source_schema(name, schema);
            }
        }
    }

    fn is_backpressured(&self) -> bool {
        let bp = self.graph.input_buf_pressure() > 0.8;
        if bp {
            self.prom.cycles_backpressured.inc();
        }
        bp
    }

    fn has_deferred_input(&self) -> bool {
        // In cluster mode, always claim pending input so the coordinator
        // runs `execute_cycle` each idle tick — otherwise a follower with
        // no local source events never drains the shuffle receiver and
        // remote rows sit stranded. Non-cluster path unchanged.
        #[cfg(feature = "cluster-unstable")]
        {
            if self.cluster_controller.is_some() {
                return true;
            }
        }
        self.graph.has_pending_input()
    }

    async fn poll_tables(&mut self) {
        use laminar_connectors::reference::RefreshMode;
        let _priority = PriorityGuard::enter(PriorityClass::BackgroundIo);

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
                            #[allow(clippy::cast_possible_truncation)]
                            let dropped = batch.num_rows() as u64;
                            self.prom.events_dropped.inc_by(dropped);
                            tracing::error!(
                                table=%name, error=%e, rows_dropped=dropped,
                                "[LDB-5030] Table upsert failed (partial); \
                                 {dropped} rows dropped"
                            );
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
                                versioned.max_versions_per_key,
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
                                max_versions_per_key: versioned.max_versions_per_key,
                            },
                        );
                        let mut ts = self.table_store.write();
                        if let Err(e) = ts.upsert_and_rebuild(name, &batch) {
                            #[allow(clippy::cast_possible_truncation)]
                            let dropped = batch.num_rows() as u64;
                            self.prom.events_dropped.inc_by(dropped);
                            tracing::error!(
                                table=%name, error=%e, rows_dropped=dropped,
                                "[LDB-5030] Table upsert failed (versioned); \
                                 {dropped} rows dropped"
                            );
                        }
                    } else {
                        let maybe_batch = {
                            let mut ts = self.table_store.write();
                            if let Err(e) = ts.upsert_and_rebuild(name, &batch) {
                                #[allow(clippy::cast_possible_truncation)]
                                let dropped = batch.num_rows() as u64;
                                self.prom.events_dropped.inc_by(dropped);
                                tracing::error!(
                                    table=%name, error=%e, rows_dropped=dropped,
                                    "[LDB-5030] Table upsert failed; \
                                     {dropped} rows dropped"
                                );
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

/// Encode an Arrow schema as a hex-encoded IPC flatbuffer for `ConnectorConfig`.
pub(crate) fn encode_arrow_schema(schema: &arrow_schema::Schema) -> String {
    laminar_connectors::config::encode_arrow_schema_ipc(schema)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DbError;

    #[tokio::test]
    async fn test_backpressure_fail_notifies_shutdown() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let err = DbError::BackpressureFail("downstream of 'q'".into());
        let msg = ConnectorPipelineCallback::map_graph_error(&err, &notify);
        assert!(msg.contains("Backpressure fail"), "unexpected: {msg}");

        tokio::time::timeout(Duration::from_millis(50), notify.notified())
            .await
            .expect("shutdown should have been notified");
    }

    #[tokio::test]
    async fn test_non_backpressure_error_does_not_notify() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let err = DbError::Pipeline("unrelated".into());
        let _ = ConnectorPipelineCallback::map_graph_error(&err, &notify);

        let got = tokio::time::timeout(Duration::from_millis(50), notify.notified()).await;
        assert!(got.is_err(), "non-Fail errors must not trigger shutdown");
    }

    /// Consumer-side cap is a no-op when the cluster has not yet
    /// published a minimum watermark — otherwise every event-time
    /// decision would freeze behind the `i64::MIN` sentinel.
    #[cfg(feature = "cluster-unstable")]
    #[test]
    fn cap_source_watermarks_none_cluster_wm_leaves_map_untouched() {
        let mut wms: FxHashMap<Arc<str>, i64> = FxHashMap::default();
        wms.insert(Arc::from("a"), 1_000);
        wms.insert(Arc::from("b"), 500);

        ConnectorPipelineCallback::cap_source_watermarks_by_cluster_min(&mut wms, None);

        assert_eq!(wms.get(&Arc::<str>::from("a")).copied(), Some(1_000));
        assert_eq!(wms.get(&Arc::<str>::from("b")).copied(), Some(500));
    }

    /// When a cluster-wide minimum is published, sources that have
    /// advanced past it get pulled back to it; sources at or below
    /// the cap are left alone (cap must not push watermarks forward).
    #[cfg(feature = "cluster-unstable")]
    #[test]
    fn cap_source_watermarks_lowers_only_sources_above_cluster_min() {
        let mut wms: FxHashMap<Arc<str>, i64> = FxHashMap::default();
        wms.insert(Arc::from("ahead"), 2_000);
        wms.insert(Arc::from("at"), 1_500);
        wms.insert(Arc::from("behind"), 800);

        ConnectorPipelineCallback::cap_source_watermarks_by_cluster_min(&mut wms, Some(1_500));

        assert_eq!(
            wms.get(&Arc::<str>::from("ahead")).copied(),
            Some(1_500),
            "source above cluster min must be capped down",
        );
        assert_eq!(
            wms.get(&Arc::<str>::from("at")).copied(),
            Some(1_500),
            "source at cluster min unchanged",
        );
        assert_eq!(
            wms.get(&Arc::<str>::from("behind")).copied(),
            Some(800),
            "source below cluster min must NOT be advanced by the cap",
        );
    }
}
