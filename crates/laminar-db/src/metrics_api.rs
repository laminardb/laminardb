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
    ///
    /// # Errors
    ///
    /// Returns an error after the database leaves `Created` or when a registry
    /// was already installed.
    pub fn set_prometheus_registry(
        &self,
        registry: Arc<prometheus::Registry>,
    ) -> Result<(), DbError> {
        let _startup = self.startup_attempt.lock();
        let mut slot = self.prometheus_registry.lock();
        let state = DbState::load(&self.state);
        if state != DbState::Created {
            return Err(DbError::InvalidOperation(format!(
                "Prometheus registry can only be installed while the database is Created, not {state:?}"
            )));
        }
        if slot.is_some() {
            return Err(DbError::InvalidOperation(
                "Prometheus registry is already installed".into(),
            ));
        }
        *slot = Some(registry);
        Ok(())
    }

    /// Get the engine metrics if set.
    #[must_use]
    pub fn engine_metrics(&self) -> Option<Arc<crate::engine_metrics::EngineMetrics>> {
        self.engine_metrics.lock().clone()
    }

    /// Read one bounded, process-bound page of local cluster checkpoint barrier timings.
    ///
    /// This is an in-memory diagnostic read. It performs no checkpoint, state-backend, network,
    /// filesystem, or object-store operation. A continuation cursor is the pair of
    /// `expected_process` and `after_sequence`; callers must retain both from the prior page.
    /// Snapshot counters may advance immediately after this call, so certification must finish
    /// with a quiescent repeated read rather than treating one page as a stable cut.
    ///
    /// # Errors
    /// Returns an error when a continuation omits or mismatches its process identity, live process
    /// authority cannot be sampled without waiting, authority changes during the read, or the
    /// bounded ledger snapshot rejects its cursor/page request.
    #[cfg(feature = "cluster")]
    pub fn checkpoint_barrier_timing_snapshot(
        &self,
        expected_process: Option<laminar_core::cluster::control::LocalProcessAuthorityIdentity>,
        after_sequence: u64,
        limit: usize,
    ) -> Result<
        crate::checkpoint_timing::CheckpointBarrierTimingPage,
        crate::checkpoint_timing::CheckpointBarrierTimingReadError,
    > {
        use crate::checkpoint_timing::{
            CheckpointBarrierTimingPage, CheckpointBarrierTimingReadError,
        };

        if after_sequence != 0 && expected_process.is_none() {
            return Err(CheckpointBarrierTimingReadError::ProcessIdentityRequired);
        }
        let controller = self
            .cluster_controller
            .try_lock()
            .and_then(|controller| controller.clone())
            .ok_or(CheckpointBarrierTimingReadError::ProcessIdentityUnavailable)?;
        let before = controller
            .try_live_local_process_authority_identity()
            .map_err(|_| CheckpointBarrierTimingReadError::ProcessIdentityUnavailable)?;
        if let Some(expected) = expected_process {
            if expected != before {
                return Err(CheckpointBarrierTimingReadError::ProcessIdentityMismatch {
                    expected,
                    actual: before,
                });
            }
        }

        let snapshot = self
            .checkpoint_barrier_timings
            .snapshot_after(after_sequence, limit)?;
        let after = controller
            .try_live_local_process_authority_identity()
            .map_err(|_| CheckpointBarrierTimingReadError::ProcessIdentityUnavailable)?;
        if before != after {
            return Err(CheckpointBarrierTimingReadError::ProcessIdentityChanged { before, after });
        }
        if let Some(actual) = snapshot.process {
            if actual != before {
                return Err(CheckpointBarrierTimingReadError::LedgerProcessMismatch {
                    expected: before,
                    actual,
                });
            }
        }
        debug_assert!(snapshot
            .records
            .iter()
            .all(|record| record.process == before));
        Ok(CheckpointBarrierTimingPage {
            process: before,
            snapshot,
        })
    }

    /// Get the current pipeline state as a string.
    pub fn pipeline_state(&self) -> &'static str {
        match DbState::load(&self.state) {
            DbState::Created => "Created",
            DbState::Starting => "Starting",
            DbState::Running => "Running",
            DbState::ShuttingDown => "ShuttingDown",
            DbState::Stopped => "Stopped",
            DbState::Faulted => "Faulted",
        }
    }

    /// The last runtime-fault or terminal resource-exhaustion reason. Populated when state is
    /// `Faulted`; cleared on a clean start.
    #[must_use]
    pub fn last_fault(&self) -> Option<String> {
        self.last_fault.lock().clone()
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
        let sql = self
            .connector_manager
            .lock()
            .streams()
            .get(name)
            .map(|reg| reg.query_sql.clone());
        Some(crate::metrics::StreamMetrics {
            name: entry.name.clone(),
            total_events: entry.emitted_rows(),
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
            DbState::Faulted => crate::metrics::PipelineState::Faulted,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "cluster")]
    async fn cluster_db_with_live_process() -> (
        Arc<LaminarDB>,
        laminar_core::cluster::control::LocalProcessAuthorityIdentity,
    ) {
        use laminar_core::cluster::control::{
            ClusterController, ClusterKv, LeaseDeadline, ProcessLeaseAuthority, ProcessLeaseOutcome,
        };
        use laminar_core::cluster::discovery::{NodeId, NodeInfo};

        let node = NodeId(71);
        let boot = uuid::Uuid::from_u128(71);
        let kv: Arc<dyn ClusterKv> =
            Arc::new(laminar_core::cluster::control::InMemoryKv::new(node));
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&kv),
            kv,
            None,
            members_rx,
            boot,
        ));
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(30),
            )))
            .unwrap();
        let authority = Arc::new(
            ProcessLeaseAuthority::new(
                Arc::new(object_store::memory::InMemory::new()),
                std::time::Duration::from_secs(30),
            )
            .unwrap(),
        );
        let ProcessLeaseOutcome::Acquired(lease) = authority
            .store_for(node)
            .try_acquire(boot, 0)
            .await
            .unwrap()
        else {
            panic!("empty process authority must grant the test process");
        };
        controller.set_process_lease_authority(authority).unwrap();
        controller
            .publish_leased_recovery_incarnation(&lease)
            .await
            .unwrap();
        let process = controller
            .try_live_local_process_authority_identity()
            .unwrap();

        let db = Arc::new(
            LaminarDB::open_with_config_and_vars_and_rules(
                crate::config::LaminarConfig::default(),
                std::collections::HashMap::new(),
                &[],
                None,
                crate::db::RuntimeMode::Cluster,
            )
            .unwrap(),
        );
        db.set_cluster_controller(controller).unwrap();
        (db, process)
    }

    #[cfg(feature = "cluster")]
    fn timing_observation(
        process: laminar_core::cluster::control::LocalProcessAuthorityIdentity,
        checkpoint_id: u64,
    ) -> crate::checkpoint_timing::CheckpointBarrierTimingObservation {
        crate::checkpoint_timing::CheckpointBarrierTimingObservation {
            process,
            attempt: laminar_core::state::CheckpointAttempt::canonical(checkpoint_id),
            role: crate::checkpoint_timing::CheckpointBarrierRole::Follower,
            assignment_version: 3,
            assignment_digest: [9; 32],
            pipeline_stall_ns: 100,
            local_barrier_ns: 60,
            aligned_resume_ns: Some(20),
            durable_tail_handoff: true,
            deadline_exhausted: false,
        }
    }

    #[test]
    fn prometheus_registry_is_single_assignment() {
        let db = LaminarDB::open().unwrap();
        db.set_prometheus_registry(Arc::new(prometheus::Registry::new()))
            .unwrap();

        let error = db
            .set_prometheus_registry(Arc::new(prometheus::Registry::new()))
            .unwrap_err()
            .to_string();

        assert!(error.contains("already installed"), "{error}");
    }

    #[test]
    fn prometheus_registry_cannot_change_after_start_begins() {
        let db = LaminarDB::open().unwrap();
        DbState::Starting.store(&db.state);

        let error = db
            .set_prometheus_registry(Arc::new(prometheus::Registry::new()))
            .unwrap_err()
            .to_string();

        DbState::Created.store(&db.state);
        assert!(error.contains("Created"), "{error}");
    }

    #[test]
    fn prometheus_registry_install_is_serialized_with_startup_claim() {
        let db = Arc::new(LaminarDB::open().unwrap());
        let startup = db.startup_attempt.lock();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let installing = Arc::clone(&db);
        let install = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            installing.set_prometheus_registry(Arc::new(prometheus::Registry::new()))
        });
        started_rx.recv().unwrap();
        DbState::Starting.store(&db.state);
        drop(startup);

        let error = install.join().unwrap().unwrap_err().to_string();

        DbState::Created.store(&db.state);
        assert!(error.contains("Created"), "{error}");
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn checkpoint_timing_cursor_is_bound_to_the_live_process() {
        use crate::checkpoint_timing::CheckpointBarrierTimingReadError;

        let (db, process) = cluster_db_with_live_process().await;
        assert!(db
            .checkpoint_barrier_timings
            .try_record(timing_observation(process, 1)));
        assert!(db
            .checkpoint_barrier_timings
            .try_record(timing_observation(process, 2)));

        assert_eq!(
            db.checkpoint_barrier_timing_snapshot(None, 1, 1),
            Err(CheckpointBarrierTimingReadError::ProcessIdentityRequired)
        );
        let mut stale_process = process;
        stale_process.process_term += 1;
        assert_eq!(
            db.checkpoint_barrier_timing_snapshot(Some(stale_process), 1, 1),
            Err(CheckpointBarrierTimingReadError::ProcessIdentityMismatch {
                expected: stale_process,
                actual: process,
            })
        );

        let first = db.checkpoint_barrier_timing_snapshot(None, 0, 1).unwrap();
        assert_eq!(first.process, process);
        assert_eq!(first.snapshot.process, Some(process));
        assert_eq!(first.snapshot.records[0].sequence, 1);
        let second = db
            .checkpoint_barrier_timing_snapshot(Some(process), 1, 1)
            .unwrap();
        assert_eq!(second.snapshot.records[0].sequence, 2);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn checkpoint_timing_read_rejects_a_foreign_ledger_domain() {
        use crate::checkpoint_timing::CheckpointBarrierTimingReadError;

        let (db, process) = cluster_db_with_live_process().await;
        let mut foreign = process;
        foreign.process_term += 1;
        assert!(db
            .checkpoint_barrier_timings
            .try_record(timing_observation(foreign, 1)));

        assert_eq!(
            db.checkpoint_barrier_timing_snapshot(None, 0, 1),
            Err(CheckpointBarrierTimingReadError::LedgerProcessMismatch {
                expected: process,
                actual: foreign,
            })
        );
    }
}
