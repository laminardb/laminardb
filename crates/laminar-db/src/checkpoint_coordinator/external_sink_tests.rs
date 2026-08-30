use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, CoordinatedCommitBatch, CoordinatedCommitContext,
    CoordinatedCommitCursor, CoordinatedCommitNamespace, CoordinatedCommitter, SinkConnector,
    SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
    MAX_COORDINATED_COMMIT_BATCH_BYTES, MAX_COORDINATED_COMMIT_PAYLOAD_BYTES,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::checkpoint::{
    checkpoint_descriptor_sha256, ByteRange, CheckpointAttempt, CheckpointManifest,
    CheckpointStore, CheckpointStoreError, ObjectStoreCheckpointStore, PipelineIdentity,
    PreparedSinkDescriptor, StateChunkId, PREPARED_SINK_DESCRIPTOR_VERSION,
};
use laminar_core::state::KeyGroupCount;
use object_store::memory::InMemory;

use super::*;

fn descriptor_manifest(participant_id: u64, payload_length: u64) -> CheckpointManifest {
    let mut manifest =
        CheckpointManifest::new_with_key_group_count(1, 1, KeyGroupCount::try_from(1_u16).unwrap());
    manifest.bind_participant(participant_id);
    manifest.prepared_sinks.push(PreparedSinkDescriptor {
        sink_name: "sink".into(),
        format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
        payload: Some(ByteRange {
            offset: 0,
            length: payload_length,
        }),
        sha256: checkpoint_descriptor_sha256(Some(&[])),
    });
    manifest
}

#[test]
fn external_sink_descriptor_bounds_precede_object_reads() {
    let oversized = u64::try_from(MAX_COORDINATED_COMMIT_PAYLOAD_BYTES).unwrap() + 1;
    let manifest = descriptor_manifest(1, oversized);
    let error = sink_commit::validated_external_sink_descriptors("sink", &[&manifest]).unwrap_err();
    assert!(error.to_string().contains("descriptor exceeds"));

    let per_participant = u64::try_from(MAX_COORDINATED_COMMIT_PAYLOAD_BYTES).unwrap();
    let manifests = (1..=5)
        .map(|participant| descriptor_manifest(participant, per_participant))
        .collect::<Vec<_>>();
    assert!(
        per_participant * u64::try_from(manifests.len()).unwrap()
            > u64::try_from(MAX_COORDINATED_COMMIT_BATCH_BYTES).unwrap()
    );
    let references = manifests.iter().collect::<Vec<_>>();
    assert_eq!(
        sink_commit::validated_external_sink_descriptors("sink", &references[..4])
            .unwrap()
            .len(),
        4
    );
    let error = sink_commit::validated_external_sink_descriptors("sink", &references).unwrap_err();
    assert!(error.to_string().contains("aggregate bytes"));
}

struct DescriptorReadGuard(Arc<AtomicUsize>);

impl Drop for DescriptorReadGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct DescriptorReadProbeStore {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl DescriptorReadProbeStore {
    fn unused<T>() -> Result<T, CheckpointStoreError> {
        Err(CheckpointStoreError::Invalid(
            "unused descriptor-read probe operation".into(),
        ))
    }
}

#[async_trait::async_trait]
impl CheckpointStore for DescriptorReadProbeStore {
    fn max_node_data_bytes(&self) -> u64 {
        laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES
    }

    async fn save_checkpoint(
        &self,
        _manifest: &CheckpointManifest,
        _node_data: &[bytes::Bytes],
    ) -> Result<bytes::Bytes, CheckpointStoreError> {
        Self::unused()
    }

    async fn seal_aborted_manifest(
        &self,
        _chunk: StateChunkId,
        _expected_artifact_identity_sha256: &str,
    ) -> Result<Option<(CheckpointManifest, bytes::Bytes)>, CheckpointStoreError> {
        Self::unused()
    }

    async fn seal_aborted_node_data(
        &self,
        _chunk: StateChunkId,
        _expected_artifact_identity_sha256: &str,
    ) -> Result<(), CheckpointStoreError> {
        Self::unused()
    }

    async fn load_manifest_for_participant(
        &self,
        _participant_id: u64,
        _checkpoint_id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        Self::unused()
    }

    async fn load_manifest_verified(
        &self,
        _participant_id: u64,
        _checkpoint_id: u64,
        _expected_len: u64,
        _expected_sha256: &str,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        Self::unused()
    }

    async fn load_node_data_ranges(
        &self,
        _chunk: StateChunkId,
        _expected_object_length: u64,
        _ranges: &[ByteRange],
    ) -> Result<Option<Vec<bytes::Bytes>>, CheckpointStoreError> {
        Self::unused()
    }

    async fn delete_manifest(&self, _chunk: StateChunkId) -> Result<(), CheckpointStoreError> {
        Self::unused()
    }

    async fn delete_node_data(&self, _chunk: StateChunkId) -> Result<(), CheckpointStoreError> {
        Self::unused()
    }

    async fn load_prepared_sink_descriptor(
        &self,
        _manifest: &CheckpointManifest,
        descriptor: &PreparedSinkDescriptor,
    ) -> Result<Option<bytes::Bytes>, CheckpointStoreError> {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak.fetch_max(active, Ordering::AcqRel);
        let _guard = DescriptorReadGuard(Arc::clone(&self.active));
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(descriptor.payload.map(|_| bytes::Bytes::from_static(b"x")))
    }
}

struct BarrierCommitSink {
    barrier: Arc<tokio::sync::Barrier>,
    commits: Arc<AtomicUsize>,
    schema: SchemaRef,
}

struct PhaseOneProbeSink {
    barrier: Arc<tokio::sync::Barrier>,
    pre_commits: Arc<AtomicUsize>,
    rollbacks: Arc<AtomicUsize>,
    fail_pre_commit: bool,
    fail_rollback: bool,
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl SinkConnector for PhaseOneProbeSink {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::CancelSafe
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(0, 0))
    }

    async fn pre_commit(&mut self, _epoch: u64) -> Result<Option<Vec<u8>>, ConnectorError> {
        self.pre_commits.fetch_add(1, Ordering::AcqRel);
        self.barrier.wait().await;
        if self.fail_pre_commit {
            return Err(ConnectorError::TransactionError(
                "injected phase-one failure".into(),
            ));
        }
        Ok(Some(vec![1]))
    }

    async fn rollback_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        self.rollbacks.fetch_add(1, Ordering::AcqRel);
        if self.fail_rollback {
            return Err(ConnectorError::TransactionError(
                "injected rollback failure".into(),
            ));
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[async_trait::async_trait]
impl SinkConnector for BarrierCommitSink {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::CancelSafe
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(0, 0))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }

    fn as_coordinated_committer(&self) -> Option<&dyn CoordinatedCommitter> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl CoordinatedCommitter for BarrierCommitSink {
    async fn commit_aggregated(
        &self,
        _batch: CoordinatedCommitBatch,
        _context: CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        self.barrier.wait().await;
        self.commits.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    async fn committed_cursor(
        &self,
        _namespace: &CoordinatedCommitNamespace,
    ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
        Ok(None)
    }
}

#[tokio::test(start_paused = true)]
async fn external_sink_descriptor_reads_use_a_bounded_window() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let store = DescriptorReadProbeStore {
        active: Arc::clone(&active),
        peak: Arc::clone(&peak),
    };
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();
    coordinator
        .bind_deployment_id("018f0000-0000-7000-8000-000000000001".into())
        .unwrap();

    let commits = Arc::new(AtomicUsize::new(0));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "sink".into(),
        sink_id: Arc::from("sink"),
        connector: Box::new(BarrierCommitSink {
            barrier: Arc::new(tokio::sync::Barrier::new(1)),
            commits: Arc::clone(&commits),
            schema: Arc::new(Schema::empty()),
        }),
        contract: SinkContract::new(
            SinkConsistency::CheckpointCommittable,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    coordinator.register_sink("sink", handle.clone());

    let manifest_count = sink_commit::MAX_EXTERNAL_SINK_DESCRIPTOR_READ_CONCURRENCY + 3;
    let manifests = (1..=u64::try_from(manifest_count).unwrap())
        .map(|participant| descriptor_manifest(participant, 1))
        .collect::<Vec<_>>();
    let references = manifests.iter().collect::<Vec<_>>();
    coordinator
        .commit_external_sinks_until(
            CheckpointAttempt::canonical(1),
            &references,
            1,
            0,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();

    assert_eq!(active.load(Ordering::Acquire), 0);
    assert_eq!(
        peak.load(Ordering::Acquire),
        sink_commit::MAX_EXTERNAL_SINK_DESCRIPTOR_READ_CONCURRENCY
    );
    assert_eq!(commits.load(Ordering::Acquire), 1);
    handle.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn committed_external_sinks_publish_concurrently() {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store =
        ObjectStoreCheckpointStore::new(objects, "parallel-external-sinks").with_participant_id(1);
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();
    coordinator
        .bind_deployment_id("018f0000-0000-7000-8000-000000000001".into())
        .unwrap();

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let commits = Arc::new(AtomicUsize::new(0));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let mut handles = Vec::new();
    for name in ["sink_a", "sink_b"] {
        let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
            name: name.into(),
            sink_id: Arc::from(name),
            connector: Box::new(BarrierCommitSink {
                barrier: Arc::clone(&barrier),
                commits: Arc::clone(&commits),
                schema: Arc::new(Schema::empty()),
            }),
            contract: SinkContract::new(
                SinkConsistency::CheckpointCommittable,
                SinkTopology::MultiWriter,
                SinkInputMode::AppendOnly,
            ),
            requires_recovery_on_error: true,
            channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
            flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
            write_timeout: Duration::from_secs(5),
            event_tx: event_tx.clone(),
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });
        coordinator.register_sink(name, handle.clone());
        handles.push(handle);
    }

    let mut manifest =
        CheckpointManifest::new_with_key_group_count(1, 1, KeyGroupCount::try_from(1_u16).unwrap());
    manifest.bind_participant(1);
    manifest.prepared_sinks = ["sink_a", "sink_b"]
        .into_iter()
        .map(|name| PreparedSinkDescriptor {
            sink_name: name.into(),
            format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
            payload: None,
            sha256: checkpoint_descriptor_sha256(None),
        })
        .collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);

    coordinator
        .commit_external_sinks_until(
            CheckpointAttempt::canonical(1),
            &[&manifest],
            1,
            0,
            deadline,
        )
        .await
        .unwrap();

    assert_eq!(commits.load(Ordering::Acquire), 2);
    assert!(tokio::time::Instant::now() < deadline);
    for handle in handles {
        handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn partial_phase_one_failure_rolls_back_every_sink_and_preserves_primary_error() {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let decisions = Arc::new(
        laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::clone(&objects)),
    );
    let store = ObjectStoreCheckpointStore::new(objects, "phase-one-rollback");
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    coordinator
        .bind_durable_decision_store(decisions)
        .await
        .unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let pre_commits = Arc::new(AtomicUsize::new(0));
    let rollbacks = Arc::new(AtomicUsize::new(0));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let mut handles = Vec::new();
    for (name, fail_pre_commit, fail_rollback) in [("sink_a", false, true), ("sink_b", true, false)]
    {
        let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
            name: name.into(),
            sink_id: Arc::from(name),
            connector: Box::new(PhaseOneProbeSink {
                barrier: Arc::clone(&barrier),
                pre_commits: Arc::clone(&pre_commits),
                rollbacks: Arc::clone(&rollbacks),
                fail_pre_commit,
                fail_rollback,
                schema: Arc::new(Schema::empty()),
            }),
            contract: SinkContract::new(
                SinkConsistency::CheckpointCommittable,
                SinkTopology::MultiWriter,
                SinkInputMode::AppendOnly,
            ),
            requires_recovery_on_error: true,
            channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
            flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
            write_timeout: Duration::from_secs(5),
            event_tx: event_tx.clone(),
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });
        coordinator.register_sink(name, handle.clone());
        handles.push(handle);
    }

    coordinator.begin_initial_epoch().await.unwrap();
    let result = coordinator
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();

    assert!(!result.success, "{result:?}");
    assert_eq!(
        result.failure_disposition,
        Some(CheckpointFailureDisposition::RequiresRecovery)
    );
    let error = result.error.as_deref().unwrap();
    let primary = error.find("sink 'sink_b' pre-commit failed").unwrap();
    let cleanup = error.find("injected rollback failure").unwrap();
    assert!(
        primary < cleanup,
        "primary error must precede cleanup: {error}"
    );
    assert_eq!(pre_commits.load(Ordering::Acquire), 2);
    assert_eq!(rollbacks.load(Ordering::Acquire), 2);
    assert!(coordinator.failure_requires_recovery);
    assert_eq!(coordinator.phase, CheckpointPhase::Idle);
    for handle in handles {
        handle.close().await.unwrap();
    }
}
