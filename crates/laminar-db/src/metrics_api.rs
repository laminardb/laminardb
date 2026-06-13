//! Pipeline metrics and state query methods for `LaminarDB`.
//!
//! Reopens `impl LaminarDB` to keep the main `db.rs` focused on dispatch.

use std::sync::Arc;

use crate::db::{DbState, LaminarDB};
use crate::error::DbError;

impl LaminarDB {
    /// Time elapsed since the database was created.
    #[must_use]
    pub fn uptime(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// Inject prometheus engine metrics. Called once at startup before `start()`.
    pub fn set_engine_metrics(&self, metrics: Arc<crate::engine_metrics::EngineMetrics>) {
        *self.engine_metrics.lock() = Some(metrics);
    }

    /// Inject a shared Prometheus registry for connector-level metrics.
    ///
    /// Called once at startup, after the registry is constructed but before
    /// `start()`. Connectors created after this call will register their
    /// metrics on this registry so they appear in the scrape output.
    pub fn set_prometheus_registry(&self, registry: Arc<prometheus::Registry>) {
        *self.prometheus_registry.lock() = Some(registry);
    }

    /// Get the engine metrics if set.
    #[must_use]
    pub fn engine_metrics(&self) -> Option<Arc<crate::engine_metrics::EngineMetrics>> {
        self.engine_metrics.lock().clone()
    }

    /// Get the current pipeline state as a string.
    pub fn pipeline_state(&self) -> &'static str {
        match DbState::load(&self.state) {
            DbState::Created => "Created",
            DbState::Starting => "Starting",
            DbState::Running => "Running",
            DbState::ShuttingDown => "ShuttingDown",
            DbState::Stopped => "Stopped",
        }
    }

    /// Get a pipeline-wide metrics snapshot.
    ///
    /// Reads prometheus engine metrics and catalog sizes to produce a
    /// point-in-time view of pipeline health.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn metrics(&self) -> crate::metrics::PipelineMetrics {
        let guard = self.engine_metrics.lock();
        let (ingested, emitted, dropped, cycles, batches, mv_updates, mv_bytes) =
            if let Some(ref m) = *guard {
                (
                    m.events_ingested.get(),
                    m.events_emitted.get(),
                    m.events_dropped.get(),
                    m.cycles.get(),
                    m.batches.get(),
                    m.mv_updates.get(),
                    m.mv_bytes_stored.get() as u64,
                )
            } else {
                (0, 0, 0, 0, 0, 0, 0)
            };
        crate::metrics::PipelineMetrics {
            total_events_ingested: ingested,
            total_events_emitted: emitted,
            total_events_dropped: dropped,
            total_cycles: cycles,
            total_batches: batches,
            uptime: self.start_time.elapsed(),
            state: self.pipeline_state_enum(),
            source_count: self.catalog.list_sources().len(),
            stream_count: self.catalog.list_streams().len(),
            sink_count: self.catalog.list_sinks().len(),
            pipeline_watermark: self.pipeline_watermark(),
            mv_updates,
            mv_bytes_stored: mv_bytes,
        }
    }

    /// Cold-tier demotion/promotion metrics — the embedded equivalent of the
    /// server's `laminardb_state_tier_*` gauges. All zero until the tier is
    /// active.
    #[cfg(feature = "state-tier")]
    #[must_use]
    pub fn tier_metrics(&self) -> crate::metrics::TierMetrics {
        let guard = self.engine_metrics.lock();
        guard
            .as_ref()
            .map_or_else(crate::metrics::TierMetrics::default, |m| {
                crate::metrics::TierMetrics {
                    demote_total: m.state_tier_demote_total.get(),
                    fetch_total: m.state_tier_fetch_total.get(),
                    resident_bytes: m.state_tier_bytes.get(),
                    resident_slices: m.state_tier_slices.get(),
                }
            })
    }

    /// Get metrics for a single source by name.
    #[must_use]
    pub fn source_metrics(&self, name: &str) -> Option<crate::metrics::SourceMetrics> {
        let entry = self.catalog.get_source(name)?;
        let pending = entry.source.pending();
        let capacity = entry.source.capacity();
        Some(crate::metrics::SourceMetrics {
            name: entry.name.clone(),
            total_events: entry.source.sequence(),
            pending,
            capacity,
            is_backpressured: crate::metrics::is_backpressured(pending, capacity),
            watermark: entry.source.current_watermark(),
            utilization: crate::metrics::utilization(pending, capacity),
        })
    }

    /// Get metrics for all registered sources.
    #[must_use]
    pub fn all_source_metrics(&self) -> Vec<crate::metrics::SourceMetrics> {
        self.catalog
            .list_sources()
            .iter()
            .filter_map(|name| self.source_metrics(name))
            .collect()
    }

    /// Get metrics for a single stream by name.
    #[must_use]
    pub fn stream_metrics(&self, name: &str) -> Option<crate::metrics::StreamMetrics> {
        let entry = self.catalog.get_stream_entry(name)?;
        let pending = entry.source.pending();
        let capacity = entry.source.capacity();
        let sql = self
            .connector_manager
            .lock()
            .streams()
            .get(name)
            .map(|reg| reg.query_sql.clone());
        Some(crate::metrics::StreamMetrics {
            name: entry.name.clone(),
            total_events: entry.source.sequence(),
            pending,
            capacity,
            is_backpressured: crate::metrics::is_backpressured(pending, capacity),
            watermark: entry.source.current_watermark(),
            sql,
        })
    }

    /// Get metrics for all registered streams.
    #[must_use]
    pub fn all_stream_metrics(&self) -> Vec<crate::metrics::StreamMetrics> {
        self.catalog
            .list_streams()
            .iter()
            .filter_map(|name| self.stream_metrics(name))
            .collect()
    }

    /// Get the total number of events processed (ingested + emitted).
    #[must_use]
    pub fn total_events_processed(&self) -> u64 {
        let guard = self.engine_metrics.lock();
        if let Some(ref m) = *guard {
            m.events_ingested.get() + m.events_emitted.get()
        } else {
            0
        }
    }

    /// Returns the global pipeline watermark (minimum across all source watermarks).
    ///
    /// Returns `i64::MIN` if no watermark-enabled sources exist or no events
    /// have been processed.
    #[must_use]
    pub fn pipeline_watermark(&self) -> i64 {
        self.pipeline_watermark
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn pipeline_state_enum(&self) -> crate::metrics::PipelineState {
        match DbState::load(&self.state) {
            DbState::Created => crate::metrics::PipelineState::Created,
            DbState::Starting => crate::metrics::PipelineState::Starting,
            DbState::Running => crate::metrics::PipelineState::Running,
            DbState::ShuttingDown => crate::metrics::PipelineState::ShuttingDown,
            DbState::Stopped => crate::metrics::PipelineState::Stopped,
        }
    }

    /// Cancel a running query by ID.
    ///
    /// Marks the query as inactive in the catalog. Future subscription
    /// polls for this query will receive no more data.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the query is not found.
    pub fn cancel_query(&self, query_id: u64) -> Result<(), DbError> {
        if self.catalog.deactivate_query(query_id) {
            Ok(())
        } else {
            Err(DbError::QueryNotFound(query_id.to_string()))
        }
    }

    /// Get the number of registered sources.
    pub fn source_count(&self) -> usize {
        self.catalog.list_sources().len()
    }

    /// Get the number of registered sinks.
    pub fn sink_count(&self) -> usize {
        self.catalog.list_sinks().len()
    }

    /// Returns checkpoint statistics if available (non-blocking).
    ///
    /// Uses `try_lock()` on the coordinator mutex. Returns `None` if
    /// the coordinator is not initialized or the lock is contended.
    pub fn checkpoint_stats_nonblocking(
        &self,
    ) -> Option<crate::checkpoint_coordinator::CheckpointStats> {
        let guard = self.coordinator.try_lock().ok()?;
        guard
            .as_ref()
            .map(crate::checkpoint_coordinator::CheckpointCoordinator::stats)
    }

    /// Get the number of active queries.
    pub fn active_query_count(&self) -> usize {
        self.catalog
            .list_queries()
            .iter()
            .filter(|(_, _, active)| *active)
            .count()
    }
}
