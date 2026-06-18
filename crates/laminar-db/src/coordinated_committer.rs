//! Decoupled designated committer: off the barrier path, the leader reads each
//! writer's descriptor for sealed epochs and runs one external commit per sink.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rustc_hash::FxHashMap;

use laminar_core::state::StateBackend;

use crate::error::DbError;
use crate::sink_task::SinkTaskHandle;

/// Commit-descriptor key for `(node, sink)` within an epoch, stored at
/// `epoch=N/commit/{key}`. Single source of truth for the build + parse pair.
pub(crate) fn descriptor_key(node_id: u64, sink: &str) -> String {
    format!("node={node_id}/sink={sink}")
}

/// The sink name a [`descriptor_key`] belongs to, or `None` if malformed.
pub(crate) fn descriptor_key_sink(key: &str) -> Option<&str> {
    key.rsplit_once("/sink=").map(|(_, sink)| sink)
}

/// Drives aggregated commits for coordinated-commit sinks on the leader.
pub(crate) struct CoordinatedCommitter {
    backend: Arc<dyn StateBackend>,
    sinks: Vec<(String, SinkTaskHandle)>,
    committed_through: FxHashMap<String, u64>,
    /// Lowest uncommitted epoch, published for the coordinator's prune clamp.
    floor: Arc<AtomicU64>,
    /// Cursors seeded from each sink's external commit state on the first pass.
    seeded: bool,
    /// Lag (sealed − committed) past which a loud warning fires.
    max_uncommitted_epochs: u64,
    metrics: Option<Arc<crate::engine_metrics::EngineMetrics>>,
    #[cfg(feature = "cluster")]
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
}

impl CoordinatedCommitter {
    pub(crate) fn new(
        backend: Arc<dyn StateBackend>,
        sinks: Vec<(String, SinkTaskHandle)>,
        floor: Arc<AtomicU64>,
    ) -> Self {
        Self {
            backend,
            sinks,
            committed_through: FxHashMap::default(),
            floor,
            seeded: false,
            max_uncommitted_epochs: u64::MAX,
            metrics: None,
            #[cfg(feature = "cluster")]
            controller: None,
        }
    }

    pub(crate) fn with_metrics(
        mut self,
        metrics: Option<Arc<crate::engine_metrics::EngineMetrics>>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    pub(crate) fn with_max_uncommitted_epochs(mut self, cap: u64) -> Self {
        self.max_uncommitted_epochs = cap;
        self
    }

    /// Restrict committing to the cluster leader (the single designated committer).
    #[cfg(feature = "cluster")]
    pub(crate) fn with_cluster_controller(
        mut self,
        controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    ) -> Self {
        self.controller = controller;
        self
    }

    /// Commit every sealed-but-uncommitted epoch. Per-sink isolated: a sink that
    /// fails stops at its cursor while others proceed; the first error is returned.
    pub(crate) async fn commit_ready(&mut self) -> Result<(), DbError> {
        // Only the designated committer (the lease-fenced leader) commits, so
        // writers never race on the shared catalog. `is_leader` is lease-aware,
        // so a stale/partitioned candidate stands down here. Drop the seed so a
        // regained leadership re-reads the catalog cursor, not a stale one.
        #[cfg(feature = "cluster")]
        if self.controller.as_ref().is_some_and(|c| !c.is_leader()) {
            self.seeded = false;
            return Ok(());
        }

        // Seed each cursor from the sink's external commit state once (so a
        // restart doesn't rescan from 0), all-or-nothing: a failed read stays
        // unseeded and retries next pass rather than committing from a stale
        // cursor left by a prior leadership term.
        if !self.seeded {
            let mut seeded = FxHashMap::default();
            for (name, handle) in &self.sinks {
                match handle.committed_through().await {
                    Ok(Some(epoch)) => {
                        seeded.insert(name.clone(), epoch);
                    }
                    Ok(None) => {} // nothing committed externally yet → cursor 0
                    Err(e) => {
                        tracing::warn!(sink = %name, error = %e,
                            "committer: cursor seed read failed; retrying next pass");
                        return Ok(());
                    }
                }
            }
            self.committed_through = seeded;
            self.seeded = true;
        }

        // One listing of sealed epochs (skips abandoned ones) serves every sink;
        // each commits the suffix of that list above its own cursor.
        let min_cursor = self.min_committed();
        let all_sealed = self
            .backend
            .sealed_epochs(min_cursor)
            .await
            .map_err(|e| DbError::Checkpoint(format!("committer: list sealed epochs: {e}")))?;
        let high = all_sealed.last().copied().unwrap_or(min_cursor);

        let mut first_err: Option<DbError> = None;
        for (name, handle) in &self.sinks {
            let cursor = self.committed_through.get(name).copied().unwrap_or(0);
            let sealed: Vec<u64> = all_sealed.iter().copied().filter(|&e| e > cursor).collect();
            let Some(&target) = sealed.last() else {
                continue;
            };
            match self.commit_sealed(handle, name, &sealed, target).await {
                Ok(()) => {
                    self.committed_through.insert(name.clone(), target);
                }
                Err(e) => {
                    first_err.get_or_insert(e); // leave the cursor; retry next pass
                }
            }
        }

        // Publish the prune floor (lowest uncommitted epoch) + lag metric, and
        // warn if the committer is falling behind so storage isn't growing blind.
        let min_committed = self.min_committed();
        self.floor.store(min_committed + 1, Ordering::Release);
        let lag = high.saturating_sub(min_committed);
        if let Some(m) = &self.metrics {
            #[allow(clippy::cast_possible_wrap)]
            m.coordinated_committer_lag_epochs.set(lag as i64);
        }
        if lag > self.max_uncommitted_epochs {
            tracing::warn!(
                lag,
                cap = self.max_uncommitted_epochs,
                "[LDB-6030] coordinated committer is falling behind — descriptors and \
                 data files are accumulating in object storage"
            );
        }

        first_err.map_or(Ok(()), Err)
    }

    /// Lowest committed epoch across sinks (0 if any sink has committed nothing).
    fn min_committed(&self) -> u64 {
        self.sinks
            .iter()
            .map(|(name, _)| self.committed_through.get(name).copied().unwrap_or(0))
            .min()
            .unwrap_or(0)
    }

    /// Commit the `sealed` epochs' descriptors for one sink as a single
    /// transaction (one snapshot), keyed by `target` (the highest of them).
    async fn commit_sealed(
        &self,
        handle: &SinkTaskHandle,
        name: &str,
        sealed: &[u64],
        target: u64,
    ) -> Result<(), DbError> {
        let mut batch: Vec<Vec<u8>> = Vec::new();
        for &epoch in sealed {
            let descriptors = self
                .backend
                .read_commit_descriptors(epoch)
                .await
                .map_err(|e| {
                    DbError::Checkpoint(format!("committer: read descriptors epoch {epoch}: {e}"))
                })?;
            batch.extend(
                descriptors
                    .into_iter()
                    .filter(|(k, _)| descriptor_key_sink(k) == Some(name))
                    .map(|(_, v)| v.to_vec()),
            );
        }
        if batch.is_empty() {
            return Ok(());
        }
        handle.commit_aggregated(target, batch).await.map_err(|e| {
            DbError::Checkpoint(format!("committer: commit through epoch {target}: {e}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use bytes::Bytes;
    use laminar_connectors::connector::{
        CoordinatedCommitter as CommitterTrait, SinkConnector, SinkConnectorCapabilities,
        WriteResult,
    };
    use laminar_connectors::error::ConnectorError;
    use laminar_core::state::{InProcessBackend, StateBackend};
    use parking_lot::Mutex;

    use crate::sink_task::{SinkTaskConfig, SinkTaskHandle, DEFAULT_CHANNEL_CAPACITY};

    type Recorded = Arc<Mutex<Vec<(u64, Vec<Vec<u8>>)>>>;

    struct RecordingSink {
        schema: SchemaRef,
        recorded: Recorded,
        committed: AtomicU64,
    }

    #[async_trait::async_trait]
    impl SinkConnector for RecordingSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }
        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(0, 0))
        }
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
        fn capabilities(&self) -> SinkConnectorCapabilities {
            SinkConnectorCapabilities::new(Duration::from_secs(5))
                .with_exactly_once()
                .with_two_phase_commit()
                .with_coordinated_commit()
        }
        fn as_coordinated_committer(&self) -> Option<&dyn CommitterTrait> {
            Some(self)
        }
    }

    #[async_trait::async_trait]
    impl CommitterTrait for RecordingSink {
        async fn commit_aggregated(
            &self,
            epoch: u64,
            descriptors: Vec<Vec<u8>>,
        ) -> Result<(), ConnectorError> {
            self.recorded.lock().push((epoch, descriptors));
            self.committed.fetch_max(epoch, Ordering::Release);
            Ok(())
        }

        async fn committed_through(&self) -> Result<Option<u64>, ConnectorError> {
            let c = self.committed.load(Ordering::Acquire);
            Ok((c > 0).then_some(c))
        }
    }

    fn spawn_recording_sink(recorded: Recorded) -> SinkTaskHandle {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let (event_tx, _rx) = laminar_core::streaming::channel::channel(
            crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY,
        );
        SinkTaskHandle::spawn(SinkTaskConfig {
            name: "ice".into(),
            sink_id: Arc::from("ice"),
            connector: Box::new(RecordingSink {
                schema,
                recorded,
                committed: AtomicU64::new(0),
            }),
            exactly_once: true,
            coordinated_commit: true,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            event_tx,
        })
    }

    async fn seal(backend: &InProcessBackend, epoch: u64, descriptor: &[u8]) {
        backend
            .write_commit_descriptor(
                epoch,
                "node=0/sink=ice",
                0,
                Bytes::copy_from_slice(descriptor),
            )
            .await
            .unwrap();
        let key = ["node=0/sink=ice".to_string()];
        assert!(backend.epoch_complete(epoch, &[], &key).await.unwrap());
    }

    #[tokio::test]
    async fn batches_sealed_epochs_into_one_commit() {
        let backend = Arc::new(InProcessBackend::new(2));
        seal(&backend, 1, b"e1").await;
        seal(&backend, 2, b"e2").await;

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let mut committer = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![("ice".into(), handle)],
            Arc::new(AtomicU64::new(0)),
        );

        committer.commit_ready().await.unwrap();

        // Both sealed epochs commit as ONE aggregated commit keyed by the high
        // epoch — one snapshot, not one per epoch.
        assert_eq!(
            recorded.lock().clone(),
            vec![(2, vec![b"e1".to_vec(), b"e2".to_vec()])]
        );

        // A second pass with no new sealed epochs is a no-op (cursor advanced).
        committer.commit_ready().await.unwrap();
        assert_eq!(recorded.lock().len(), 1);
    }

    #[tokio::test]
    async fn skips_abandoned_epoch_with_partial_descriptor() {
        let backend = Arc::new(InProcessBackend::new(2));
        seal(&backend, 1, b"e1").await;
        // Epoch 2 wrote a descriptor but was abandoned (never sealed).
        backend
            .write_commit_descriptor(2, "node=0/sink=ice", 0, Bytes::from_static(b"orphan"))
            .await
            .unwrap();
        seal(&backend, 3, b"e3").await;

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let mut committer = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![("ice".into(), handle)],
            Arc::new(AtomicU64::new(0)),
        );

        committer.commit_ready().await.unwrap();

        // Epochs 1 and 3 batch into one commit keyed by 3; epoch 2's orphan
        // descriptor (never sealed) must NOT be committed.
        assert_eq!(
            recorded.lock().clone(),
            vec![(3, vec![b"e1".to_vec(), b"e3".to_vec()])]
        );
    }

    /// On restart/failover a fresh committer seeds its cursor from the sink's
    /// external commit state and must not re-commit already-committed epochs.
    #[tokio::test]
    async fn restart_resumes_from_committed_through() {
        let backend = Arc::new(InProcessBackend::new(2));
        seal(&backend, 1, b"e1").await;
        seal(&backend, 2, b"e2").await;

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));

        let mut first = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![("ice".into(), handle.clone())],
            Arc::new(AtomicU64::new(0)),
        );
        first.commit_ready().await.unwrap();
        assert_eq!(recorded.lock().len(), 1);

        // Fresh committer (restart) over the same sink — seeds from committed_through.
        let mut restarted = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![("ice".into(), handle)],
            Arc::new(AtomicU64::new(0)),
        );
        restarted.commit_ready().await.unwrap();
        assert_eq!(
            recorded.lock().len(),
            1,
            "restart must not re-commit already-committed epochs"
        );
    }
}
