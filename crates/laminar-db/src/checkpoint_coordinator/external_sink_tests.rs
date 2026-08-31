use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, CoordinatedAbortBatch, CoordinatedAbortCleaner,
    CoordinatedAbortDescriptor, CoordinatedCommitBatch, CoordinatedCommitContext,
    CoordinatedCommitCursor, CoordinatedCommitNamespace, CoordinatedCommitter, SinkConnector,
    SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
    MAX_COORDINATED_COMMIT_BATCH_BYTES, MAX_COORDINATED_COMMIT_PAYLOAD_BYTES,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::checkpoint::{
    checkpoint_artifact_identity_sha256, checkpoint_artifact_intent_sha256,
    checkpoint_descriptor_sha256, checkpoint_sha256, ByteRange, CheckpointAttempt,
    CheckpointManifest, CheckpointManifestAbortSeal, CheckpointSinkArtifactIntent, CheckpointStore,
    CheckpointStoreError, ObjectStoreCheckpointStore, PipelineIdentity, PreparedSinkArtifactIntent,
    PreparedSinkDescriptor, StateChunkId, PREPARED_SINK_DESCRIPTOR_VERSION,
};
use laminar_core::state::KeyGroupCount;
use object_store::memory::InMemory;
use object_store::ObjectStoreExt;

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

#[tokio::test]
async fn external_sink_abort_bounds_precede_object_reads() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let store = DescriptorReadProbeStore {
        active: Arc::clone(&active),
        peak: Arc::clone(&peak),
    };
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    let cleanups = Arc::new(AtomicUsize::new(0));
    let (handle, _events) = register_cleanup_probe(&mut coordinator, cleanups, false);

    let oversized = u64::try_from(MAX_COORDINATED_COMMIT_PAYLOAD_BYTES).unwrap() + 1;
    let mut manifest = descriptor_manifest(1, 1);
    manifest
        .sink_artifact_intents
        .push(PreparedSinkArtifactIntent {
            sink_name: "sink".into(),
            format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
            payload: Some(ByteRange {
                offset: 1,
                length: oversized,
            }),
            sha256: checkpoint_artifact_intent_sha256(Some(&[])),
        });
    let open_intents = std::collections::BTreeMap::from([(1, Some(Vec::new()))]);
    let error = coordinator
        .load_external_sink_abort_entries(
            &coordinator.sinks[0],
            CheckpointAttempt::canonical(1),
            &[1],
            &[&manifest],
            &open_intents,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("abort payload exceeds"));
    assert_eq!(active.load(Ordering::Acquire), 0);
    assert_eq!(peak.load(Ordering::Acquire), 0);
    handle.close().await.unwrap();
}

#[tokio::test]
async fn artifact_cleanup_fails_closed_without_a_detached_cleaner() {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (mut coordinator, _) =
        durable_cleanup_coordinator(objects, "missing-detached-abort-cleaner").await;
    let cleanups = Arc::new(AtomicUsize::new(0));
    let (handle, _events) = register_cleanup_probe(&mut coordinator, Arc::clone(&cleanups), false);
    coordinator.sinks[0].abort_cleaner = None;
    let intent = CheckpointSinkArtifactIntent::try_new("sink".into(), Some(vec![1]))
        .expect("test artifact intent must be valid");
    let open_intents = std::collections::BTreeMap::from([(1, Some(vec![intent]))]);

    let error = coordinator
        .cleanup_aborted_external_sinks_until(
            CheckpointAttempt::canonical(1),
            &[1],
            &[],
            &open_intents,
            1,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("LDB-CHECKPOINT-ABORT-CLEANER-MISSING"));
    assert_eq!(cleanups.load(Ordering::Acquire), 0);
    handle.close().await.unwrap();
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

    async fn save_sink_artifact_intents(
        &self,
        _chunk: StateChunkId,
        _expected_artifact_identity_sha256: &str,
        _intents: Vec<CheckpointSinkArtifactIntent>,
    ) -> Result<(), CheckpointStoreError> {
        Self::unused()
    }

    async fn seal_aborted_manifest(
        &self,
        _chunk: StateChunkId,
        _expected_artifact_identity_sha256: &str,
        _sink_artifact_intent_protocol: bool,
    ) -> Result<CheckpointManifestAbortSeal, CheckpointStoreError> {
        Self::unused()
    }

    async fn complete_aborted_sink_cleanup(
        &self,
        _chunk: StateChunkId,
        _expected_artifact_identity_sha256: &str,
    ) -> Result<CheckpointManifestAbortSeal, CheckpointStoreError> {
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

    async fn load_sink_artifact_intent(
        &self,
        _manifest: &CheckpointManifest,
        intent: &PreparedSinkArtifactIntent,
    ) -> Result<Option<bytes::Bytes>, CheckpointStoreError> {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak.fetch_max(active, Ordering::AcqRel);
        let _guard = DescriptorReadGuard(Arc::clone(&self.active));
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(intent.payload.map(|_| bytes::Bytes::from_static(b"x")))
    }
}

struct BarrierCommitSink {
    barrier: Arc<tokio::sync::Barrier>,
    commits: Arc<AtomicUsize>,
    cleanups: Arc<AtomicUsize>,
    fail_cleanup: bool,
    expected_cleanup_payload: Option<&'static [u8]>,
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

struct AtLeastOncePhaseProbeSink {
    flushes: Arc<AtomicUsize>,
    schema: SchemaRef,
}

struct ArtifactIntentOrderSink {
    objects: Arc<dyn object_store::ObjectStore>,
    manifest_path: object_store::path::Path,
    begins: Arc<AtomicUsize>,
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl SinkConnector for ArtifactIntentOrderSink {
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

    async fn checkpoint_artifact_intent(
        &mut self,
        epoch: u64,
    ) -> Result<Option<Vec<u8>>, ConnectorError> {
        Ok(Some(format!("intent-for-{epoch}").into_bytes()))
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        let bytes = self
            .objects
            .get(&self.manifest_path)
            .await
            .map_err(|error| ConnectorError::TransactionError(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| ConnectorError::TransactionError(error.to_string()))?;
        let record: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|error| ConnectorError::TransactionError(error.to_string()))?;
        let expected = serde_json::to_value(format!("intent-for-{epoch}").into_bytes())
            .map_err(|error| ConnectorError::TransactionError(error.to_string()))?;
        if record["sink_intents"][0]["payload"] != expected {
            return Err(ConnectorError::TransactionError(
                "sink begin observed no exact durable artifact intent".into(),
            ));
        }
        self.begins.fetch_add(1, Ordering::AcqRel);
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
impl SinkConnector for AtLeastOncePhaseProbeSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(0, 0))
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Err(ConnectorError::Internal(
            "durable at-least-once sink entered begin-epoch".into(),
        ))
    }

    async fn pre_commit(&mut self, _epoch: u64) -> Result<Option<Vec<u8>>, ConnectorError> {
        Err(ConnectorError::Internal(
            "durable at-least-once sink entered pre-commit".into(),
        ))
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        self.flushes.fetch_add(1, Ordering::AcqRel);
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

#[async_trait::async_trait]
impl CoordinatedAbortCleaner for BarrierCommitSink {
    async fn cleanup_aborted(
        &self,
        batch: CoordinatedAbortBatch,
        _context: CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        batch
            .validate_shape()
            .map_err(ConnectorError::TransactionError)?;
        let prepared_payload = batch
            .entries
            .first()
            .and_then(|entry| match &entry.descriptor {
                CoordinatedAbortDescriptor::Prepared(Some(payload)) => Some(payload.as_slice()),
                CoordinatedAbortDescriptor::Open | CoordinatedAbortDescriptor::Prepared(None) => {
                    None
                }
            });
        if self
            .expected_cleanup_payload
            .is_some_and(|expected| batch.entries.len() != 1 || prepared_payload != Some(expected))
        {
            return Err(ConnectorError::TransactionError(
                "cleanup did not receive the durable participant descriptor".into(),
            ));
        }
        self.cleanups.fetch_add(1, Ordering::AcqRel);
        if self.fail_cleanup {
            return Err(ConnectorError::TransactionError(
                "injected aborted artifact cleanup failure".into(),
            ));
        }
        Ok(())
    }
}

#[tokio::test]
async fn sink_artifact_intent_is_durable_before_connector_begin() {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let decisions = Arc::new(
        laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::clone(&objects)),
    );
    let prefix = "intent-before-begin";
    let store = ObjectStoreCheckpointStore::new(Arc::clone(&objects), prefix);
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    coordinator
        .bind_durable_decision_store(decisions)
        .await
        .unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();

    let begins = Arc::new(AtomicUsize::new(0));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "sink".into(),
        sink_id: Arc::from("sink"),
        connector: Box::new(ArtifactIntentOrderSink {
            objects,
            manifest_path: object_store::path::Path::from(format!(
                "{prefix}/nodes/1/checkpoints/00000000000000000001/manifest.json"
            )),
            begins: Arc::clone(&begins),
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

    coordinator.begin_initial_epoch().await.unwrap();

    assert_eq!(begins.load(Ordering::Acquire), 1);
    handle.rollback_epoch(1).await.unwrap();
    handle.close().await.unwrap();
}

#[tokio::test]
async fn durable_at_least_once_sink_stays_out_of_coordinated_protocol() {
    let store = ObjectStoreCheckpointStore::new(
        Arc::new(InMemory::new()),
        "at-least-once-coordinator-regression",
    );
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    let flushes = Arc::new(AtomicUsize::new(0));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "sink".into(),
        sink_id: Arc::from("sink"),
        connector: Box::new(AtLeastOncePhaseProbeSink {
            flushes: Arc::clone(&flushes),
            schema: Arc::new(Schema::empty()),
        }),
        contract: SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            SinkTopology::Singleton,
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    coordinator.begin_initial_epoch().await.unwrap();
    let descriptors = coordinator
        .pre_commit_sinks_until(1, deadline)
        .await
        .unwrap();
    coordinator
        .commit_external_sinks_until(CheckpointAttempt::canonical(1), &[], 0, 0, deadline)
        .await
        .unwrap();

    assert!(descriptors.is_empty());
    assert_eq!(flushes.load(Ordering::Acquire), 1);
    handle.close().await.unwrap();
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
    let cleanups = Arc::new(AtomicUsize::new(0));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "sink".into(),
        sink_id: Arc::from("sink"),
        connector: Box::new(BarrierCommitSink {
            barrier: Arc::new(tokio::sync::Barrier::new(1)),
            commits: Arc::clone(&commits),
            cleanups,
            fail_cleanup: false,
            expected_cleanup_payload: None,
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
    let cleanups = Arc::new(AtomicUsize::new(0));
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
                cleanups: Arc::clone(&cleanups),
                fail_cleanup: false,
                expected_cleanup_payload: None,
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

async fn durable_cleanup_coordinator(
    objects: Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> (
    CheckpointCoordinator,
    Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
) {
    let decisions = Arc::new(
        laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::clone(&objects)),
    );
    let store = ObjectStoreCheckpointStore::new(objects, prefix)
        .with_key_group_count(KeyGroupCount::try_from(1_u16).unwrap());
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    coordinator
        .bind_durable_decision_store(Arc::clone(&decisions))
        .await
        .unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();
    (coordinator, decisions)
}

fn durable_sink_manifest(
    attempt: CheckpointAttempt,
    deployment_id: &str,
    payload: &[u8],
) -> CheckpointManifest {
    let mut manifest = CheckpointManifest::new_with_key_group_count(
        attempt.epoch,
        attempt.checkpoint_id,
        KeyGroupCount::try_from(1_u16).unwrap(),
    );
    manifest.deployment_id = deployment_id.into();
    manifest.sink_names = vec!["sink".into()];
    manifest.prepared_sinks.push(PreparedSinkDescriptor {
        sink_name: "sink".into(),
        format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
        payload: Some(ByteRange {
            offset: 0,
            length: u64::try_from(payload.len()).unwrap(),
        }),
        sha256: checkpoint_descriptor_sha256(Some(payload)),
    });
    manifest
        .sink_artifact_intents
        .push(PreparedSinkArtifactIntent {
            sink_name: "sink".into(),
            format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
            payload: None,
            sha256: checkpoint_artifact_intent_sha256(None),
        });
    manifest.node_data.object_length = u64::try_from(payload.len()).unwrap();
    manifest.node_data.sha256 = checkpoint_sha256(payload);
    manifest
}

async fn persist_durable_cleanup_descriptor(
    coordinator: &CheckpointCoordinator,
    attempt: CheckpointAttempt,
    payload: &'static [u8],
) -> (
    CheckpointManifest,
    laminar_core::checkpoint_decision::CheckpointArtifactInventory,
) {
    coordinator
        .begin_checkpoint_artifacts_until(
            attempt,
            None,
            None,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    let inventory = coordinator
        .checkpoint_artifact_inventory(attempt, None)
        .unwrap();
    let manifest = durable_sink_manifest(
        attempt,
        coordinator.expected_deployment_id().unwrap(),
        payload,
    );
    let identity = checkpoint_artifact_identity_sha256(&inventory, manifest.node_data.chunk)
        .expect("test artifact identity must be valid");
    coordinator
        .store
        .save_sink_artifact_intents(
            manifest.node_data.chunk,
            &identity,
            vec![CheckpointSinkArtifactIntent::try_new("sink".into(), None)
                .expect("test sink intent must be valid")],
        )
        .await
        .unwrap();
    coordinator
        .store
        .save_checkpoint(&manifest, &[bytes::Bytes::from_static(payload)])
        .await
        .unwrap();
    (manifest, inventory)
}

fn register_cleanup_probe(
    coordinator: &mut CheckpointCoordinator,
    cleanups: Arc<AtomicUsize>,
    fail_cleanup: bool,
) -> (
    crate::sink_task::SinkTaskHandle,
    laminar_core::streaming::channel::AsyncConsumer<crate::sink_task::SinkEvent>,
) {
    let (event_tx, event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let barrier = Arc::new(tokio::sync::Barrier::new(1));
    let commits = Arc::new(AtomicUsize::new(0));
    let schema = Arc::new(Schema::empty());
    let abort_cleaner: Arc<dyn CoordinatedAbortCleaner> = Arc::new(BarrierCommitSink {
        barrier: Arc::clone(&barrier),
        commits: Arc::clone(&commits),
        cleanups: Arc::clone(&cleanups),
        fail_cleanup,
        expected_cleanup_payload: Some(b"durable-descriptor"),
        schema: Arc::clone(&schema),
    });
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "sink".into(),
        sink_id: Arc::from("sink"),
        connector: Box::new(BarrierCommitSink {
            barrier,
            commits,
            cleanups,
            fail_cleanup,
            expected_cleanup_payload: Some(b"durable-descriptor"),
            schema,
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
    coordinator.register_sink_with_abort_cleaner("sink", handle.clone(), Some(abort_cleaner));
    (handle, event_rx)
}

#[tokio::test]
async fn recovery_skips_cleanup_for_a_participant_that_never_reached_begin() {
    const PREFIX: &str = "unadmitted-aborted-sink-cleanup";

    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (mut interrupted, decisions) =
        durable_cleanup_coordinator(Arc::clone(&objects), PREFIX).await;
    let (interrupted_handle, _events) =
        register_cleanup_probe(&mut interrupted, Arc::new(AtomicUsize::new(0)), false);
    let attempt = CheckpointAttempt::canonical(1);
    interrupted
        .begin_checkpoint_artifacts_until(
            attempt,
            None,
            None,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    interrupted_handle.close().await.unwrap();
    drop(interrupted);

    let cleanups = Arc::new(AtomicUsize::new(0));
    let (mut restarted, _) = durable_cleanup_coordinator(objects, PREFIX).await;
    let (handle, _events) = register_cleanup_probe(&mut restarted, Arc::clone(&cleanups), true);

    assert!(restarted.recover().await.unwrap().is_none());
    assert_eq!(cleanups.load(Ordering::Acquire), 0);
    assert!(decisions
        .checkpoint_decision_head()
        .await
        .unwrap()
        .unwrap()
        .active_artifacts
        .is_none());
    handle.close().await.unwrap();
}

#[tokio::test]
async fn recovery_rejects_legacy_open_participants_with_unknown_artifact_state() {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = ObjectStoreCheckpointStore::new(objects, "legacy-open-sink-cleanup");
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    let cleanups = Arc::new(AtomicUsize::new(0));
    let (handle, _events) = register_cleanup_probe(&mut coordinator, cleanups, false);
    let legacy = std::collections::BTreeMap::from([(1_u64, None)]);

    let error = coordinator
        .load_external_sink_abort_entries(
            &coordinator.sinks[0],
            CheckpointAttempt::canonical(1),
            &[1],
            &[],
            &legacy,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("LDB-CHECKPOINT-LEGACY-SINK-INTENT"));
    handle.close().await.unwrap();
}

#[tokio::test]
async fn current_sink_intent_persistence_rejects_a_legacy_artifact_identity() {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (mut coordinator, _) =
        durable_cleanup_coordinator(objects, "legacy-sink-intent-admission").await;
    let (handle, _events) =
        register_cleanup_probe(&mut coordinator, Arc::new(AtomicUsize::new(0)), false);
    let mut inventory = coordinator
        .checkpoint_artifact_inventory(CheckpointAttempt::canonical(1), None)
        .unwrap();
    assert!(inventory.sink_artifact_intent_protocol);
    inventory.sink_artifact_intent_protocol = false;

    let error = coordinator
        .persist_sink_artifact_intents_until(
            &inventory,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("LDB-CHECKPOINT-LEGACY-SINK-INTENT"));
    handle.close().await.unwrap();
}

#[tokio::test]
async fn recovery_preserves_legacy_attempts_without_artifact_intent_proof() {
    const PREFIX: &str = "legacy-unresolved-sink-cleanup";

    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (interrupted, decisions) = durable_cleanup_coordinator(Arc::clone(&objects), PREFIX).await;
    let attempt = CheckpointAttempt::canonical(1);
    let mut legacy = interrupted
        .checkpoint_artifact_inventory(attempt, None)
        .unwrap();
    legacy.sink_artifact_intent_protocol = false;
    decisions
        .begin_checkpoint_artifact_inventory(legacy)
        .await
        .unwrap();
    drop(interrupted);

    let cleanups = Arc::new(AtomicUsize::new(0));
    let (mut restarted, _) = durable_cleanup_coordinator(objects, PREFIX).await;
    let (handle, _events) = register_cleanup_probe(&mut restarted, Arc::clone(&cleanups), false);
    let error = restarted.recover().await.unwrap_err();

    assert!(error
        .to_string()
        .contains("LDB-CHECKPOINT-LEGACY-SINK-INTENT"));
    assert_eq!(cleanups.load(Ordering::Acquire), 0);
    assert!(decisions
        .checkpoint_decision_head()
        .await
        .unwrap()
        .unwrap()
        .active_artifacts
        .is_some());
    handle.close().await.unwrap();
}

#[tokio::test]
async fn aborted_sink_cleanup_retries_before_destroying_durable_descriptors() {
    const PREFIX: &str = "durable-aborted-sink-cleanup";
    const DESCRIPTOR: &[u8] = b"durable-descriptor";

    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (mut interrupted, decisions) =
        durable_cleanup_coordinator(Arc::clone(&objects), PREFIX).await;
    let (interrupted_handle, _events) =
        register_cleanup_probe(&mut interrupted, Arc::new(AtomicUsize::new(0)), false);
    let attempt = CheckpointAttempt::canonical(1);
    let (manifest, _) = persist_durable_cleanup_descriptor(&interrupted, attempt, DESCRIPTOR).await;
    interrupted_handle.close().await.unwrap();
    drop(interrupted);

    let cleanups = Arc::new(AtomicUsize::new(0));
    let (mut first_restart, _) = durable_cleanup_coordinator(Arc::clone(&objects), PREFIX).await;
    let (first_handle, _first_events) =
        register_cleanup_probe(&mut first_restart, Arc::clone(&cleanups), true);
    let error = first_restart.recover().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("injected aborted artifact cleanup failure"));
    assert_eq!(cleanups.load(Ordering::Acquire), 1);

    let inventory = decisions
        .checkpoint_decision_head()
        .await
        .unwrap()
        .unwrap()
        .active_artifacts
        .unwrap();
    let identity =
        checkpoint_artifact_identity_sha256(&inventory, manifest.node_data.chunk).unwrap();
    let seal = first_restart
        .store
        .seal_aborted_manifest(
            manifest.node_data.chunk,
            &identity,
            inventory.sink_artifact_intent_protocol,
        )
        .await
        .unwrap();
    assert!(!seal.sink_cleanup_complete);
    assert_eq!(
        first_restart
            .store
            .load_prepared_sink_descriptor(&manifest, &manifest.prepared_sinks[0])
            .await
            .unwrap()
            .as_deref(),
        Some(DESCRIPTOR)
    );
    first_handle.close().await.unwrap();
    drop(first_restart);

    let (mut second_restart, _) = durable_cleanup_coordinator(Arc::clone(&objects), PREFIX).await;
    let (second_handle, _second_events) =
        register_cleanup_probe(&mut second_restart, Arc::clone(&cleanups), false);
    assert!(second_restart.recover().await.unwrap().is_none());
    assert_eq!(cleanups.load(Ordering::Acquire), 2);
    assert!(decisions
        .checkpoint_decision_head()
        .await
        .unwrap()
        .unwrap()
        .active_artifacts
        .is_none());
    assert!(second_restart
        .store
        .load_prepared_sink_descriptor(&manifest, &manifest.prepared_sinks[0])
        .await
        .is_err());
    second_handle.close().await.unwrap();
}

#[tokio::test]
async fn completed_abort_cleanup_is_not_repeated_after_restart() {
    const PREFIX: &str = "completed-aborted-sink-cleanup";
    const DESCRIPTOR: &[u8] = b"durable-descriptor";

    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (mut interrupted, decisions) =
        durable_cleanup_coordinator(Arc::clone(&objects), PREFIX).await;
    let (interrupted_handle, _events) =
        register_cleanup_probe(&mut interrupted, Arc::new(AtomicUsize::new(0)), false);
    let attempt = CheckpointAttempt::canonical(1);
    let (manifest, inventory) =
        persist_durable_cleanup_descriptor(&interrupted, attempt, DESCRIPTOR).await;
    let identity =
        checkpoint_artifact_identity_sha256(&inventory, manifest.node_data.chunk).unwrap();
    interrupted
        .store
        .seal_aborted_manifest(
            manifest.node_data.chunk,
            &identity,
            inventory.sink_artifact_intent_protocol,
        )
        .await
        .unwrap();
    let completed = interrupted
        .store
        .complete_aborted_sink_cleanup(manifest.node_data.chunk, &identity)
        .await
        .unwrap();
    assert!(completed.sink_cleanup_complete);
    interrupted_handle.close().await.unwrap();
    drop(interrupted);

    let cleanups = Arc::new(AtomicUsize::new(0));
    let (mut restarted, _) = durable_cleanup_coordinator(Arc::clone(&objects), PREFIX).await;
    let (handle, _events) = register_cleanup_probe(&mut restarted, Arc::clone(&cleanups), true);
    assert!(restarted.recover().await.unwrap().is_none());
    assert_eq!(cleanups.load(Ordering::Acquire), 0);
    assert!(decisions
        .checkpoint_decision_head()
        .await
        .unwrap()
        .unwrap()
        .active_artifacts
        .is_none());
    assert!(restarted
        .store
        .load_prepared_sink_descriptor(&manifest, &manifest.prepared_sinks[0])
        .await
        .is_err());
    handle.close().await.unwrap();
}
