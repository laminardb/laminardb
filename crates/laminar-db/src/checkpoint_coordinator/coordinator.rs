use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_core::checkpoint::{
    CheckpointAttempt, CheckpointManifest, CheckpointStore, CommittedCheckpointRef,
    ConnectorCheckpoint, PipelineIdentity,
};
use tracing::warn;

use super::retention::run_gc_worker;
use super::{
    CheckpointConfig, CheckpointCoordinator, CheckpointPhase, DbError, EpochAllocator,
    REFERENCED_CHUNK_REBASE_THRESHOLD,
};

impl CheckpointCoordinator {
    pub fn new(config: CheckpointConfig, store: Box<dyn CheckpointStore>) -> Result<Self, DbError> {
        laminar_core::checkpoint::checkpoint_store::validate_max_checkpoint_node_data_bytes(
            config.max_node_data_bytes,
        )
        .map_err(|error| DbError::Config(format!("checkpoint.max_node_data_bytes: {error}")))?;
        if store.max_node_data_bytes() != config.max_node_data_bytes {
            return Err(DbError::Config(format!(
                "checkpoint store node-data limit {} does not match checkpoint.max_node_data_bytes {}",
                store.max_node_data_bytes(),
                config.max_node_data_bytes
            )));
        }
        if store.participant_id() == 0 {
            return Err(DbError::Config(
                "checkpoint participant ID must be nonzero".into(),
            ));
        }
        let vnode_count = store.key_group_count().get();
        #[cfg(feature = "cluster")]
        let recovery_graph_payload_limit =
            usize::try_from(config.max_node_data_bytes).unwrap_or(usize::MAX);
        let store: Arc<dyn CheckpointStore> = Arc::from(store);
        let (gc_requests, gc_receiver) = tokio::sync::watch::channel(None);
        let gc_task = tokio::spawn(run_gc_worker(Arc::clone(&store), gc_receiver));
        Ok(Self {
            allocator: Arc::new(EpochAllocator::new()),
            config,
            store,
            phase: CheckpointPhase::Idle,
            sinks: Vec::new(),
            assignment_version: 0,
            assignment_scoped_sources: HashSet::new(),
            owned_vnodes: (0..u32::from(vnode_count)).collect(),
            pipeline_identity: None,
            deployment_id: None,
            decision_store: None,
            active_sink_witness: None,
            prepared: HashMap::new(),
            last_committed_manifest: None,
            last_committed_ref: None,
            last_committed_source_watermarks: None,
            failure_requires_recovery: false,
            local_watermark: laminar_core::checkpoint::CheckpointWatermark::Uninitialized,
            #[cfg(feature = "cluster")]
            cluster_controller: None,
            #[cfg(feature = "cluster")]
            recovery_graph_payload_limit,
            checkpoints_completed: 0,
            checkpoints_failed: 0,
            last_checkpoint_duration: None,
            duration_histogram: DurationHistogram::new(),
            total_bytes_written: 0,
            prom: None,
            gc_requests,
            gc_task,
        })
    }

    pub fn set_decision_store(
        &mut self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<(), DbError> {
        self.allocator.bind_decision_store(Arc::clone(&store))?;
        self.decision_store = Some(store);
        Ok(())
    }

    pub async fn bind_durable_decision_store(
        &mut self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<(), DbError> {
        let deployment_id = store
            .load_or_create_deployment_id()
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("load checkpoint deployment identity: {error}"))
            })?;
        self.set_decision_store(store)?;
        self.bind_deployment_id(deployment_id)
    }

    pub(crate) fn bind_pipeline_identity(
        &mut self,
        identity: PipelineIdentity,
    ) -> Result<(), DbError> {
        match self.pipeline_identity.as_ref() {
            None => {
                self.pipeline_identity = Some(identity);
                Ok(())
            }
            Some(current) if current == &identity => Ok(()),
            Some(_) => Err(DbError::Checkpoint(
                "pipeline identity cannot change while checkpointing is active".into(),
            )),
        }
    }

    pub(crate) fn bound_pipeline_identity(&self) -> Result<PipelineIdentity, DbError> {
        self.pipeline_identity.clone().ok_or_else(|| {
            DbError::Checkpoint("checkpoint coordinator has no pipeline identity".into())
        })
    }

    pub(crate) fn bind_deployment_id(&mut self, deployment_id: String) -> Result<(), DbError> {
        let canonical = uuid::Uuid::parse_str(&deployment_id)
            .is_ok_and(|id| !id.is_nil() && id.to_string() == deployment_id);
        if !canonical {
            return Err(DbError::Checkpoint(
                "checkpoint deployment identity must be a canonical non-nil UUID".into(),
            ));
        }
        match self.deployment_id.as_ref() {
            None => {
                self.deployment_id = Some(deployment_id);
                Ok(())
            }
            Some(current) if current == &deployment_id => Ok(()),
            Some(_) => Err(DbError::Checkpoint(
                "deployment identity cannot change while checkpointing is active".into(),
            )),
        }
    }

    pub(super) fn expected_pipeline_identity(&self) -> Result<PipelineIdentity, DbError> {
        self.bound_pipeline_identity()
    }

    pub(super) fn expected_deployment_id(&self) -> Result<&str, DbError> {
        self.deployment_id.as_deref().ok_or_else(|| {
            DbError::Checkpoint("checkpoint deployment identity is not bound".into())
        })
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn bound_deployment_id(&self) -> Result<&str, DbError> {
        self.expected_deployment_id()
    }

    #[cfg(feature = "cluster")]
    pub fn set_cluster_controller(
        &mut self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
    ) {
        self.cluster_controller = Some(controller);
    }

    #[cfg(feature = "cluster")]
    #[must_use]
    pub(crate) fn participant_id(&self) -> u64 {
        self.store.participant_id()
    }

    pub fn set_assignment_version(&mut self, version: u64) {
        self.assignment_version = version;
    }

    pub(crate) fn set_assignment_scoped_sources(
        &mut self,
        sources: impl IntoIterator<Item = String>,
    ) {
        self.assignment_scoped_sources = sources.into_iter().collect();
    }

    pub fn set_vnode_set(&mut self, mut vnodes: Vec<u32>) {
        vnodes.sort_unstable();
        vnodes.dedup();
        self.owned_vnodes = vnodes;
    }

    pub fn set_local_watermark(
        &mut self,
        watermark: laminar_core::checkpoint::CheckpointWatermark,
    ) {
        self.local_watermark = watermark;
    }

    pub fn set_metrics(&mut self, prom: Arc<crate::engine_metrics::EngineMetrics>) {
        self.prom = Some(prom);
    }

    fn emit_checkpoint_metrics(
        &self,
        success: bool,
        attempt: CheckpointAttempt,
        duration: Duration,
        checkpoint_bytes: Option<u64>,
    ) {
        let Some(metrics) = self.prom.as_ref() else {
            return;
        };
        if success {
            metrics.checkpoints_completed.inc();
            if let Some(checkpoint_bytes) = checkpoint_bytes {
                metrics
                    .checkpoint_size_bytes
                    .set(i64::try_from(checkpoint_bytes).unwrap_or(i64::MAX));
            }
        } else {
            metrics.checkpoints_failed.inc();
            warn!(
                checkpoint_id = attempt.checkpoint_id,
                epoch = attempt.epoch,
                "checkpoint failure metric recorded"
            );
        }
        metrics
            .checkpoint_epoch
            .set(i64::try_from(attempt.epoch).unwrap_or(i64::MAX));
        metrics.checkpoint_duration.observe(duration.as_secs_f64());
    }

    pub(super) fn record_checkpoint_outcome(
        &mut self,
        success: bool,
        attempt: CheckpointAttempt,
        duration: Duration,
        checkpoint_bytes: Option<u64>,
    ) {
        if success {
            self.checkpoints_completed = self.checkpoints_completed.saturating_add(1);
        } else {
            self.checkpoints_failed = self.checkpoints_failed.saturating_add(1);
        }
        self.last_checkpoint_duration = Some(duration);
        self.duration_histogram.record(duration);
        self.emit_checkpoint_metrics(success, attempt, duration, checkpoint_bytes);
    }

    #[must_use]
    pub fn phase(&self) -> CheckpointPhase {
        self.phase
    }

    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.allocator.peek_epoch()
    }

    #[must_use]
    pub fn config(&self) -> &CheckpointConfig {
        &self.config
    }

    #[must_use]
    pub(crate) fn epoch_allocator(&self) -> Arc<EpochAllocator> {
        Arc::clone(&self.allocator)
    }

    #[must_use]
    pub fn store(&self) -> &dyn CheckpointStore {
        self.store.as_ref()
    }
}

impl std::fmt::Debug for CheckpointCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CheckpointCoordinator")
            .field("phase", &self.phase)
            .field("participant", &self.store.participant_id())
            .field("sinks", &self.sinks.len())
            .field("completed", &self.checkpoints_completed)
            .field("failed", &self.checkpoints_failed)
            .finish_non_exhaustive()
    }
}

impl Drop for CheckpointCoordinator {
    fn drop(&mut self) {
        self.gc_task.abort();
    }
}

pub(super) struct DurationHistogram {
    samples: Box<[u64; Self::CAPACITY]>,
    cursor: usize,
    count: usize,
}

impl DurationHistogram {
    const CAPACITY: usize = 100;

    fn new() -> Self {
        Self {
            samples: Box::new([0; Self::CAPACITY]),
            cursor: 0,
            count: 0,
        }
    }

    fn record(&mut self, duration: Duration) {
        self.samples[self.cursor] = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        self.cursor = (self.cursor + 1) % Self::CAPACITY;
        self.count = self.count.saturating_add(1).min(Self::CAPACITY);
    }

    fn percentiles(&self) -> (u64, u64, u64) {
        if self.count == 0 {
            return (0, 0, 0);
        }
        let mut values = self.samples[..self.count].to_vec();
        values.sort_unstable();
        let at = |numerator: usize| {
            let index = (self.count.saturating_sub(1) * numerator).div_ceil(100);
            values[index]
        };
        (at(50), at(95), at(99))
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CheckpointStats {
    pub completed: u64,
    pub failed: u64,
    pub last_duration: Option<Duration>,
    pub duration_p50_ms: u64,
    pub duration_p95_ms: u64,
    pub duration_p99_ms: u64,
    pub total_bytes_written: u64,
    pub current_phase: CheckpointPhase,
    pub current_epoch: u64,
}

impl CheckpointCoordinator {
    #[cfg(feature = "cluster")]
    pub(crate) fn last_committed_ref(&self) -> Option<&CommittedCheckpointRef> {
        self.last_committed_ref.as_ref()
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) fn set_last_committed_ref_for_test(&mut self, reference: CommittedCheckpointRef) {
        self.last_committed_source_watermarks = None;
        self.last_committed_ref = Some(reference);
    }

    pub(crate) fn last_committed_manifest(&self) -> Option<&CheckpointManifest> {
        self.last_committed_manifest.as_deref()
    }

    pub(crate) fn committed_manifest_needs_vnode_rebase(&self, attempt: CheckpointAttempt) -> bool {
        self.last_committed_manifest
            .as_ref()
            .is_some_and(|manifest| {
                manifest.checkpoint_id == attempt.checkpoint_id
                    && manifest.epoch == attempt.epoch
                    && manifest.referenced_chunks.len() >= REFERENCED_CHUNK_REBASE_THRESHOLD
            })
    }

    #[must_use]
    pub fn stats(&self) -> CheckpointStats {
        let (p50, p95, p99) = self.duration_histogram.percentiles();
        CheckpointStats {
            completed: self.checkpoints_completed,
            failed: self.checkpoints_failed,
            last_duration: self.last_checkpoint_duration,
            duration_p50_ms: p50 / 1_000,
            duration_p95_ms: p95 / 1_000,
            duration_p99_ms: p99 / 1_000,
            total_bytes_written: self.total_bytes_written,
            current_phase: self.phase,
            current_epoch: self.allocator.peek_epoch(),
        }
    }
}

#[must_use]
pub(crate) fn source_to_connector_checkpoint(cp: &SourceCheckpoint) -> ConnectorCheckpoint {
    ConnectorCheckpoint {
        offsets: cp.durable_offsets(),
        metadata: cp.metadata().clone(),
        input_channels: cp.input_channels().map(<[Vec<u8>]>::to_vec),
        source_assignment_version: cp.assignment_version(),
    }
}

#[must_use]
pub(crate) fn connector_to_source_checkpoint(cp: &ConnectorCheckpoint) -> SourceCheckpoint {
    let mut source = SourceCheckpoint::with_offsets(cp.offsets.clone());
    for (key, value) in &cp.metadata {
        source.set_metadata(key.clone(), value.clone());
    }
    if let Some(channels) = &cp.input_channels {
        source
            .set_input_channels(channels.clone())
            .expect("validated connector checkpoint input-channel inventory");
    }
    if let Some(version) = cp.source_assignment_version {
        source.bind_assignment_version(version);
    }
    source
}
