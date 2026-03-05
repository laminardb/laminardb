//! Pipeline metrics and state query methods for `LaminarDB`.
//!
//! Reopens `impl LaminarDB` to keep the main `db.rs` focused on dispatch.

use std::sync::Arc;

use crate::db::{
    LaminarDB, STATE_CREATED, STATE_RUNNING, STATE_SHUTTING_DOWN, STATE_STARTING, STATE_STOPPED,
};
use crate::error::DbError;

impl LaminarDB {
    /// Get the current pipeline state as a string.
    pub fn pipeline_state(&self) -> &'static str {
        match self.state.load(std::sync::atomic::Ordering::Acquire) {
            STATE_CREATED => "Created",
            STATE_STARTING => "Starting",
            STATE_RUNNING => "Running",
            STATE_SHUTTING_DOWN => "ShuttingDown",
            STATE_STOPPED => "Stopped",
            _ => "Unknown",
        }
    }

    /// Get a pipeline-wide metrics snapshot.
    ///
    /// Reads shared atomic counters and catalog sizes to produce a
    /// point-in-time view of pipeline health.
    #[must_use]
    pub fn metrics(&self) -> crate::metrics::PipelineMetrics {
        let snap = self.counters.snapshot();
        crate::metrics::PipelineMetrics {
            total_events_ingested: snap.events_ingested,
            total_events_emitted: snap.events_emitted,
            total_events_dropped: snap.events_dropped,
            total_cycles: snap.cycles,
            total_batches: snap.total_batches,
            uptime: self.start_time.elapsed(),
            state: self.pipeline_state_enum(),
            last_cycle_duration_ns: snap.last_cycle_duration_ns,
            source_count: self.catalog.list_sources().len(),
            stream_count: self.catalog.list_streams().len(),
            sink_count: self.catalog.list_sinks().len(),
            pipeline_watermark: self.pipeline_watermark(),
        }
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
        let snap = self.counters.snapshot();
        snap.events_ingested + snap.events_emitted
    }

    /// Get a reference to the shared pipeline counters.
    ///
    /// Useful for external code that needs to read counters directly
    /// (e.g. a TUI dashboard polling at high frequency).
    #[must_use]
    pub fn counters(&self) -> &Arc<crate::metrics::PipelineCounters> {
        &self.counters
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

    /// Convert the internal `AtomicU8` state to a `PipelineState` enum.
    pub(crate) fn pipeline_state_enum(&self) -> crate::metrics::PipelineState {
        match self.state.load(std::sync::atomic::Ordering::Acquire) {
            STATE_CREATED => crate::metrics::PipelineState::Created,
            STATE_STARTING => crate::metrics::PipelineState::Starting,
            STATE_RUNNING => crate::metrics::PipelineState::Running,
            STATE_SHUTTING_DOWN => crate::metrics::PipelineState::ShuttingDown,
            _ => crate::metrics::PipelineState::Stopped,
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

    /// Get the number of active queries.
    pub fn active_query_count(&self) -> usize {
        self.catalog
            .list_queries()
            .iter()
            .filter(|(_, _, active)| *active)
            .count()
    }
}
