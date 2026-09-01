#[cfg(feature = "cluster")]
use std::time::Instant;

#[cfg(feature = "cluster")]
use super::recovery::recovery_sink_fence;
use super::retention::{
    delete_retired_data, load_cleanup_target, load_protected_checkpoint, run_gc_request,
    GcAuthority, GcRequest,
};
use super::*;
use laminar_core::checkpoint::ObjectStoreCheckpointStore;
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::PREPARED_SINK_DESCRIPTOR_VERSION;
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::{
    checkpoint_artifact_intent_sha256, CheckpointAssignmentFence, CheckpointParticipant,
    CheckpointSinkArtifactIntent,
};
use laminar_core::checkpoint_decision::{
    CheckpointDecisionStore, CheckpointRetentionState, CheckpointRetentionUpdateResult,
};
#[cfg(feature = "cluster")]
use laminar_core::cluster::control::{
    BarrierAck, BarrierAckDisposition, ClusterController, ClusterKv, InMemoryKv, LeaderLeaseOwner,
    LeaderLeaseStore, LeaseDeadline, LeaseOutcome, ACK_KEY,
};
#[cfg(feature = "cluster")]
use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
use laminar_core::state::KeyGroupCount;
use object_store::memory::InMemory;
#[cfg(feature = "cluster")]
use object_store::ObjectStoreExt;

#[cfg(feature = "cluster")]
pub(super) struct CommitThenIoStore {
    pub(super) inner: Arc<dyn object_store::ObjectStore>,
    pub(super) lose_put_ack: std::sync::atomic::AtomicBool,
    pub(super) path_suffix: &'static str,
    pub(super) block_get: std::sync::atomic::AtomicBool,
    pub(super) deny_list: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "cluster")]
impl std::fmt::Debug for CommitThenIoStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommitThenIoStore")
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "cluster")]
impl std::fmt::Display for CommitThenIoStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CommitThenIoStore")
    }
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl object_store::ObjectStore for CommitThenIoStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let eligible = location.to_string().ends_with(self.path_suffix);
        let result = self.inner.put_opts(location, payload, options).await?;
        if eligible
            && self
                .lose_put_ack
                .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(object_store::Error::Generic {
                store: "CommitThenIoStore",
                source: Box::new(std::io::Error::other(
                    "injected manifest acknowledgement loss after conditional put",
                )),
            });
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if self.block_get.load(std::sync::atomic::Ordering::Acquire) {
            return std::future::pending().await;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        if self.deny_list.load(std::sync::atomic::Ordering::Acquire) {
            return Box::pin(futures::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "CommitThenIoStore",
                    source: Box::new(std::io::Error::other("object listing is forbidden")),
                })
            }));
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(feature = "cluster")]
struct AmbiguousFollowerSink {
    rollbacks: Arc<std::sync::atomic::AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
    expected_admission: Option<(Arc<LeaderLeaseStore>, CheckpointAttempt, LeaderProof)>,
    expected_intent: Option<(Arc<dyn object_store::ObjectStore>, object_store::path::Path)>,
}

#[cfg(feature = "cluster")]
struct ClusterAbortCleanupSink {
    cleanups: Arc<std::sync::atomic::AtomicU64>,
    expected_fencing_token: u64,
    schema: arrow::datatypes::SchemaRef,
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for ClusterAbortCleanupSink {
    fn cancellation_policy(&self) -> laminar_connectors::connector::ConnectorCancellationPolicy {
        laminar_connectors::connector::ConnectorCancellationPolicy::CancelSafe
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::record_batch::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Ok(laminar_connectors::connector::WriteResult::new(0, 0))
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn as_coordinated_committer(
        &self,
    ) -> Option<&dyn laminar_connectors::connector::CoordinatedCommitter> {
        Some(self)
    }
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl laminar_connectors::connector::CoordinatedCommitter for ClusterAbortCleanupSink {
    async fn commit_aggregated(
        &self,
        _batch: laminar_connectors::connector::CoordinatedCommitBatch,
        _context: laminar_connectors::connector::CoordinatedCommitContext,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Err(laminar_connectors::error::ConnectorError::InvalidState {
            expected: "abort cleanup only".into(),
            actual: "external publication".into(),
        })
    }

    async fn committed_cursor(
        &self,
        _namespace: &laminar_connectors::connector::CoordinatedCommitNamespace,
    ) -> Result<
        Option<laminar_connectors::connector::CoordinatedCommitCursor>,
        laminar_connectors::error::ConnectorError,
    > {
        Ok(None)
    }
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl laminar_connectors::connector::CoordinatedAbortCleaner for ClusterAbortCleanupSink {
    async fn cleanup_aborted(
        &self,
        batch: laminar_connectors::connector::CoordinatedAbortBatch,
        _context: laminar_connectors::connector::CoordinatedCommitContext,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        batch
            .validate_shape()
            .map_err(laminar_connectors::error::ConnectorError::TransactionError)?;
        if batch.fencing_token != self.expected_fencing_token
            || batch.entries.len() != 2
            || batch.entries.iter().any(|entry| {
                !matches!(
                    &entry.descriptor,
                    laminar_connectors::connector::CoordinatedAbortDescriptor::Prepared(None)
                ) || entry.artifact_intent.is_some()
            })
        {
            return Err(laminar_connectors::error::ConnectorError::InvalidState {
                expected: "current leader fence and two empty participant markers".into(),
                actual: format!(
                    "fence {}, {} participant markers",
                    batch.fencing_token,
                    batch.entries.len()
                ),
            });
        }
        self.cleanups
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }
}

#[cfg(feature = "cluster")]
fn register_cluster_cleanup_probe(
    coordinator: &mut CheckpointCoordinator,
    cleanups: Arc<std::sync::atomic::AtomicU64>,
    expected_fencing_token: u64,
) -> (
    crate::sink_task::SinkTaskHandle,
    laminar_core::streaming::channel::AsyncConsumer<crate::sink_task::SinkEvent>,
) {
    let (event_tx, event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let schema = Arc::new(arrow::datatypes::Schema::empty());
    let abort_cleaner: Arc<dyn laminar_connectors::connector::CoordinatedAbortCleaner> =
        Arc::new(ClusterAbortCleanupSink {
            cleanups: Arc::clone(&cleanups),
            expected_fencing_token,
            schema: Arc::clone(&schema),
        });
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "probe".into(),
        sink_id: Arc::from("probe"),
        connector: Box::new(ClusterAbortCleanupSink {
            cleanups,
            expected_fencing_token,
            schema,
        }),
        contract: laminar_connectors::connector::SinkContract::new(
            laminar_connectors::connector::SinkConsistency::CheckpointCommittable,
            laminar_connectors::connector::SinkTopology::MultiWriter,
            laminar_connectors::connector::SinkInputMode::AppendOnly,
        ),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(1),
        event_tx,
        terminal_tasks: None,
        process_authority: None,
    });
    coordinator.register_sink_with_abort_cleaner("probe", handle.clone(), Some(abort_cleaner));
    (handle, event_rx)
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for AmbiguousFollowerSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::record_batch::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Ok(laminar_connectors::connector::WriteResult {
            records_written: 0,
            bytes_written: 0,
        })
    }

    async fn begin_epoch(
        &mut self,
        epoch: u64,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        if let Some((objects, path)) = &self.expected_intent {
            let bytes = objects
                .get(path)
                .await
                .map_err(|_| {
                    laminar_connectors::error::ConnectorError::ConnectionFailed(
                        "durable sink artifact intent read failed".into(),
                    )
                })?
                .bytes()
                .await
                .map_err(|_| {
                    laminar_connectors::error::ConnectorError::ConnectionFailed(
                        "durable sink artifact intent body read failed".into(),
                    )
                })?;
            let record: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                laminar_connectors::error::ConnectorError::TransactionError(
                    "durable sink artifact intent is invalid".into(),
                )
            })?;
            let admitted = record["sink_intents"].as_array().is_some_and(|intents| {
                intents.len() == 1
                    && intents[0]["sink_name"] == "probe"
                    && intents[0]["payload"].is_null()
            });
            if !admitted {
                return Err(laminar_connectors::error::ConnectorError::InvalidState {
                    expected: "durable current-protocol sink artifact intent before begin".into(),
                    actual: "missing or different intent envelope".into(),
                });
            }
        }
        let Some((authority, expected_attempt, expected_proof)) = &self.expected_admission else {
            return Ok(());
        };
        let admission = authority
            .cluster_checkpoint_artifact_admission()
            .await
            .map_err(|error| {
                laminar_connectors::error::ConnectorError::ConnectionFailed(error.to_string())
            })?;
        if admission.as_ref().is_some_and(|(inventory, proof)| {
            inventory.attempt == *expected_attempt
                && proof == expected_proof
                && inventory.attempt.epoch == epoch
        }) {
            return Ok(());
        }
        Err(laminar_connectors::error::ConnectorError::InvalidState {
            expected: format!(
                "durable checkpoint admission for {expected_attempt:?} before sink begin"
            ),
            actual: format!("{admission:?}"),
        })
    }

    async fn rollback_epoch(
        &mut self,
        _epoch: u64,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        self.rollbacks
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }
}

fn manifest(
    checkpoint_id: u64,
    deployment_id: &str,
    retained_chunk: Option<(StateChunkId, u64, String)>,
) -> (CheckpointManifest, Bytes) {
    let key_groups = KeyGroupCount::try_from(1_u16).unwrap();
    let mut manifest =
        CheckpointManifest::new_with_key_group_count(checkpoint_id, checkpoint_id, key_groups);
    manifest.deployment_id = deployment_id.into();
    let payload;
    let frame_chunk;
    if let Some((chunk, object_length, sha256)) = retained_chunk {
        payload = Bytes::new();
        frame_chunk = chunk;
        manifest.referenced_chunks.push(ReferencedStateChunk {
            chunk,
            object_length,
            sha256,
            ref_count: NonZeroU32::new(1).unwrap(),
        });
    } else {
        payload = Bytes::from(vec![u8::try_from(checkpoint_id).unwrap()]);
        frame_chunk = manifest.node_data.chunk;
    }
    manifest.state_frames.push(StateFrame {
        key: StateFrameKey::Vnode {
            operator_id: "join".into(),
            vnode: 0,
        },
        chunk: frame_chunk,
        range: ByteRange {
            offset: 0,
            length: 1,
        },
        sha256: checkpoint_sha256(&[u8::try_from(frame_chunk.checkpoint_id).unwrap()]),
    });
    manifest.node_data.object_length = payload.len() as u64;
    manifest.node_data.sha256 = checkpoint_sha256(&payload);
    (manifest, payload)
}

#[cfg(feature = "cluster")]
fn cluster_manifest(
    checkpoint_id: u64,
    participant_id: u64,
    vnode: u16,
    deployment_id: &str,
    fence: &CheckpointAssignmentFence,
    key_groups: KeyGroupCount,
) -> (CheckpointManifest, Bytes) {
    let payload = Bytes::from(vec![
        u8::try_from(checkpoint_id).unwrap(),
        u8::try_from(participant_id).unwrap(),
    ]);
    let mut manifest =
        CheckpointManifest::new_with_key_group_count(checkpoint_id, checkpoint_id, key_groups);
    manifest.bind_participant(participant_id);
    manifest.deployment_id = deployment_id.into();
    manifest.assignment_fence = Some(fence.clone());
    manifest.reassignment_portable = true;
    manifest.owned_vnodes = vec![vnode];
    manifest.state_frames.push(StateFrame {
        key: StateFrameKey::Vnode {
            operator_id: "join".into(),
            vnode,
        },
        chunk: manifest.node_data.chunk,
        range: ByteRange {
            offset: 0,
            length: payload.len() as u64,
        },
        sha256: checkpoint_sha256(&payload),
    });
    manifest.node_data.object_length = payload.len() as u64;
    manifest.node_data.sha256 = checkpoint_sha256(&payload);
    (manifest, payload)
}

#[cfg(feature = "cluster")]
async fn save_cluster_manifests(
    objects: Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    checkpoint_id: u64,
    deployment_id: &str,
    fence: &CheckpointAssignmentFence,
    key_groups: KeyGroupCount,
    prepared_sink: Option<&str>,
) -> Vec<(CheckpointManifest, Bytes)> {
    let mut manifests = Vec::with_capacity(2);
    for (participant_id, vnode) in [(1, 0), (2, 1)] {
        let store = ObjectStoreCheckpointStore::new(Arc::clone(&objects), prefix)
            .with_key_group_count(key_groups)
            .with_participant_id(participant_id);
        let (mut manifest, payload) = cluster_manifest(
            checkpoint_id,
            participant_id,
            vnode,
            deployment_id,
            fence,
            key_groups,
        );
        if let Some(sink_name) = prepared_sink {
            manifest.sink_names = vec![sink_name.into()];
            manifest
                .sink_artifact_intents
                .push(PreparedSinkArtifactIntent {
                    sink_name: sink_name.into(),
                    format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
                    payload: None,
                    sha256: checkpoint_artifact_intent_sha256(None),
                });
            manifest.prepared_sinks.push(PreparedSinkDescriptor {
                sink_name: sink_name.into(),
                format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
                payload: None,
                sha256: checkpoint_descriptor_sha256(None),
            });
            let inventory = CheckpointArtifactInventory {
                deployment_id: deployment_id.into(),
                pipeline_identity: manifest.pipeline_identity.clone(),
                attempt: CheckpointAttempt::canonical(checkpoint_id),
                assignment_fence: Some(fence.clone()),
                sink_artifact_intent_protocol: true,
            };
            let identity =
                checkpoint_artifact_identity_sha256(&inventory, manifest.node_data.chunk).unwrap();
            store
                .save_sink_artifact_intents(
                    manifest.node_data.chunk,
                    &identity,
                    vec![CheckpointSinkArtifactIntent::try_new(sink_name.into(), None).unwrap()],
                )
                .await
                .unwrap();
        }
        let encoded = store
            .save_checkpoint(&manifest, std::slice::from_ref(&payload))
            .await
            .unwrap();
        manifests.push((manifest, encoded));
    }
    manifests
}

fn retention_state(result: CheckpointRetentionUpdateResult) -> CheckpointRetentionState {
    match result {
        CheckpointRetentionUpdateResult::Applied(state)
        | CheckpointRetentionUpdateResult::Unchanged(state) => state,
        result => panic!("unexpected retention update: {result:?}"),
    }
}

async fn node_data_exists(
    store: &dyn CheckpointStore,
    chunk: StateChunkId,
    object_length: u64,
) -> bool {
    store
        .load_node_data_ranges(chunk, object_length, &[])
        .await
        .unwrap()
        .is_some()
}

async fn admit_local_artifacts(
    decisions: &CheckpointDecisionStore,
    deployment_id: &str,
    checkpoint_id: u64,
    pipeline_identity: PipelineIdentity,
) {
    let result = decisions
        .begin_checkpoint_artifact_inventory(CheckpointArtifactInventory {
            deployment_id: deployment_id.to_owned(),
            pipeline_identity,
            attempt: CheckpointAttempt::canonical(checkpoint_id),
            assignment_fence: None,
            sink_artifact_intent_protocol: true,
        })
        .await
        .unwrap();
    assert!(matches!(
        result,
        CheckpointArtifactInventoryUpdateResult::Applied
            | CheckpointArtifactInventoryUpdateResult::Unchanged
    ));
}

#[tokio::test]
async fn retention_reclaims_last_referenced_chunk_and_keeps_latest_cut() {
    use laminar_core::checkpoint_decision::CheckpointVerdict;

    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let key_groups = KeyGroupCount::try_from(1_u16).unwrap();
    let store: Arc<dyn CheckpointStore> = Arc::new(
        ObjectStoreCheckpointStore::new(Arc::clone(&objects), "gc")
            .with_key_group_count(key_groups),
    );
    let decisions = Arc::new(CheckpointDecisionStore::new(objects));
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

    let mut predecessor = None;
    let mut latest = None;
    let mut checkpoint_three = None;
    for checkpoint_id in 1..=6 {
        admit_local_artifacts(
            decisions.as_ref(),
            &deployment_id,
            checkpoint_id,
            PipelineIdentity::empty(),
        )
        .await;
        let retained_chunk = (checkpoint_id == 6)
            .then(|| checkpoint_three.clone().expect("checkpoint three metadata"));
        let (manifest, payload) = manifest(checkpoint_id, &deployment_id, retained_chunk);
        store
            .save_checkpoint(&manifest, std::slice::from_ref(&payload))
            .await
            .unwrap();
        if checkpoint_id == 3 {
            checkpoint_three = Some((
                manifest.node_data.chunk,
                manifest.node_data.object_length,
                manifest.node_data.sha256.clone(),
            ));
        }
        let manifest_bytes = checkpoint_manifest_bytes(&manifest).unwrap();
        let participant =
            CommittedParticipantRef::from_manifest(&manifest, &manifest_bytes).unwrap();
        let index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id: deployment_id.clone(),
            pipeline_identity: manifest.pipeline_identity.clone(),
            epoch: checkpoint_id,
            checkpoint_id,
            scope: CheckpointScope::Local,
            vnode_count: 1,
            assignment_fence: None,
            reassignment_portable: false,
            predecessor,
            participants: vec![participant],
            source_names: Vec::new(),
            source_offsets: BTreeMap::new(),
            channel_progress: Vec::new(),
            source_watermarks: BTreeMap::new(),
            checkpoint_watermark: None,
        };
        let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
        decisions
            .record_outcome(
                checkpoint_id,
                checkpoint_id,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                Some(reference.clone()),
            )
            .await
            .unwrap();
        predecessor = Some(reference);
        latest = Some(index);
    }

    let protected = predecessor.clone().unwrap();
    let interrupted = retention_state(
        decisions
            .begin_checkpoint_retention(&protected)
            .await
            .unwrap(),
    );
    let CheckpointRetentionState::DeleteData { cursor } = interrupted else {
        panic!("retention did not enter its data phase");
    };
    assert_eq!(cursor.current.checkpoint_id, 5);
    store
        .delete_node_data(StateChunkId {
            participant_id: 1,
            checkpoint_id: 5,
        })
        .await
        .unwrap();

    run_gc_request(
        Arc::clone(&store),
        GcRequest {
            requested: Some(latest.take().unwrap()),
            decision_store: Arc::clone(&decisions),
            authority: GcAuthority::Local,
            #[cfg(feature = "cluster")]
            metrics: None,
        },
    )
    .await
    .unwrap();

    for checkpoint_id in [1, 2, 4, 5] {
        let chunk = StateChunkId {
            participant_id: 1,
            checkpoint_id,
        };
        assert!(!node_data_exists(store.as_ref(), chunk, 1).await);
    }
    assert!(
        node_data_exists(
            store.as_ref(),
            StateChunkId {
                participant_id: 1,
                checkpoint_id: 3,
            },
            1,
        )
        .await
    );
    for checkpoint_id in 1..=5 {
        assert_eq!(
            store
                .load_manifest_for_participant(1, checkpoint_id)
                .await
                .unwrap(),
            None
        );
    }
    assert!(store
        .load_manifest_for_participant(1, 6)
        .await
        .unwrap()
        .is_some());

    admit_local_artifacts(
        decisions.as_ref(),
        &deployment_id,
        7,
        PipelineIdentity::empty(),
    )
    .await;
    let (manifest, payload) = manifest(7, &deployment_id, None);
    store
        .save_checkpoint(&manifest, std::slice::from_ref(&payload))
        .await
        .unwrap();
    let encoded = checkpoint_manifest_bytes(&manifest).unwrap();
    let index = CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id,
        pipeline_identity: manifest.pipeline_identity.clone(),
        epoch: 7,
        checkpoint_id: 7,
        scope: CheckpointScope::Local,
        vnode_count: 1,
        assignment_fence: None,
        reassignment_portable: false,
        predecessor,
        participants: vec![CommittedParticipantRef::from_manifest(&manifest, &encoded).unwrap()],
        source_names: Vec::new(),
        source_offsets: BTreeMap::new(),
        channel_progress: Vec::new(),
        source_watermarks: BTreeMap::new(),
        checkpoint_watermark: None,
    };
    let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
    decisions
        .record_outcome(
            7,
            7,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            Some(reference.clone()),
        )
        .await
        .unwrap();

    let delete_data = retention_state(
        decisions
            .begin_checkpoint_retention(&reference)
            .await
            .unwrap(),
    );
    let CheckpointRetentionState::DeleteData { cursor } = &delete_data else {
        panic!("retention did not enter its data phase");
    };
    let retained = load_protected_checkpoint(store.as_ref(), decisions.as_ref(), &reference)
        .await
        .unwrap();
    let (_, retired) = load_cleanup_target(
        store.as_ref(),
        decisions.as_ref(),
        &retained.index,
        &cursor.current,
        cursor.next.as_ref(),
        Some(&[1]),
    )
    .await
    .unwrap();
    delete_retired_data(store.as_ref(), &retired, &retained.live)
        .await
        .unwrap();
    let delete_metadata = retention_state(
        decisions
            .advance_checkpoint_retention(&delete_data)
            .await
            .unwrap(),
    );
    assert!(matches!(
        delete_metadata,
        CheckpointRetentionState::DeleteMetadata { .. }
    ));
    store
        .delete_manifest(StateChunkId {
            participant_id: 1,
            checkpoint_id: 6,
        })
        .await
        .unwrap();
    run_gc_request(
        Arc::clone(&store),
        GcRequest {
            requested: Some(index),
            decision_store: decisions,
            authority: GcAuthority::Local,
            #[cfg(feature = "cluster")]
            metrics: None,
        },
    )
    .await
    .unwrap();

    for checkpoint_id in [3, 6] {
        let object_length = u64::from(checkpoint_id == 3);
        assert!(
            !node_data_exists(
                store.as_ref(),
                StateChunkId {
                    participant_id: 1,
                    checkpoint_id,
                },
                object_length,
            )
            .await
        );
    }
    assert!(store
        .load_manifest_for_participant(1, 7)
        .await
        .unwrap()
        .is_some());
}

async fn coordinator_with_store(
    objects: Arc<dyn object_store::ObjectStore>,
) -> (CheckpointCoordinator, Arc<CheckpointDecisionStore>, String) {
    let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&objects)));
    let store = ObjectStoreCheckpointStore::new(objects, "aborted-artifacts")
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
    let deployment_id = coordinator.expected_deployment_id().unwrap().to_owned();
    (coordinator, decisions, deployment_id)
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn committed_index_binds_sorted_participants_predecessor_and_assignment_fence() {
    let key_groups = KeyGroupCount::try_from(2_u16).unwrap();
    let deployment_id = uuid::Uuid::from_u128(1).to_string();
    let fence = CheckpointAssignmentFence::from_owner_map(
        9,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
        ],
    )
    .unwrap();
    let store = ObjectStoreCheckpointStore::new(Arc::new(InMemory::new()), "index-bindings")
        .with_key_group_count(key_groups)
        .with_participant_id(1);
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();
    coordinator
        .bind_deployment_id(deployment_id.clone())
        .unwrap();

    let manifests = [(1, 0), (2, 1)]
        .into_iter()
        .map(|(participant_id, vnode)| {
            let (manifest, _) =
                cluster_manifest(2, participant_id, vnode, &deployment_id, &fence, key_groups);
            let encoded = Bytes::from(checkpoint_manifest_bytes(&manifest).unwrap());
            (manifest, encoded)
        })
        .collect::<Vec<_>>();
    let predecessor = CommittedCheckpointRef {
        epoch: 1,
        checkpoint_id: 1,
        sha256: checkpoint_sha256(b"predecessor"),
        len: 1,
    };

    let index = coordinator
        .build_committed_index(
            CheckpointAttempt::canonical(2),
            CheckpointScope::Cluster,
            Some(fence.clone()),
            Some(predecessor.clone()),
            &BTreeMap::new(),
            &manifests,
            None,
        )
        .unwrap();

    assert_eq!(
        index
            .participants
            .iter()
            .map(|participant| participant.participant_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(index.predecessor, Some(predecessor));
    assert_eq!(index.assignment_fence, Some(fence));
    index.validate().unwrap();
}

#[tokio::test]
async fn initial_committed_index_derives_an_empty_inventory_source_cut_from_its_marker() {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (coordinator, decisions, deployment_id) = coordinator_with_store(objects).await;
    let (mut manifest, _) = manifest(1, &deployment_id, None);
    manifest.source_names = vec!["orders".into()];
    manifest.source_offsets.insert(
        "orders".into(),
        ConnectorCheckpoint {
            input_channels: Some(Vec::new()),
            ..ConnectorCheckpoint::default()
        },
    );
    manifest.channel_progress = vec![ChannelProgress {
        participant_id: laminar_core::state::LOCAL_NODE_ID.0,
        source_name: "orders".into(),
        input_channel: laminar_core::checkpoint::SINGLETON_WATERMARK_CHANNEL.to_vec(),
        watermark: Some(900),
        idle: true,
    }];
    let encoded = Bytes::from(checkpoint_manifest_bytes(&manifest).unwrap());

    let index = coordinator
        .build_committed_index(
            CheckpointAttempt::canonical(1),
            CheckpointScope::Local,
            None,
            None,
            &BTreeMap::new(),
            &[(manifest, encoded)],
            None,
        )
        .unwrap();
    assert_eq!(index.version, COMMITTED_CHECKPOINT_INDEX_VERSION);
    assert!(index.predecessor.is_none());
    assert_eq!(index.source_watermarks.get("orders"), Some(&900));
    assert_eq!(index.checkpoint_watermark, None);
    assert!(index.source_offsets["orders"]
        .input_channels
        .as_ref()
        .is_some_and(Vec::is_empty));
    index.validate().unwrap();

    let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
    let restored = decisions
        .load_committed_checkpoint(&reference)
        .await
        .unwrap();
    assert_eq!(restored.source_watermarks.get("orders"), Some(&900));
    assert_eq!(restored, index);
}

#[tokio::test]
async fn committed_index_retains_a_predecessor_cut_for_an_empty_source_inventory() {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (coordinator, decisions, deployment_id) = coordinator_with_store(objects).await;
    let (mut predecessor_manifest, _) = manifest(1, &deployment_id, None);
    predecessor_manifest.source_names = vec!["orders".into()];
    predecessor_manifest.channel_progress = vec![ChannelProgress {
        participant_id: laminar_core::state::LOCAL_NODE_ID.0,
        source_name: "orders".into(),
        input_channel: laminar_core::checkpoint::SINGLETON_WATERMARK_CHANNEL.to_vec(),
        watermark: Some(900),
        idle: false,
    }];
    predecessor_manifest.checkpoint_watermark = Some(900);
    let predecessor_bytes = Bytes::from(checkpoint_manifest_bytes(&predecessor_manifest).unwrap());
    let predecessor_index = coordinator
        .build_committed_index(
            CheckpointAttempt::canonical(1),
            CheckpointScope::Local,
            None,
            None,
            &BTreeMap::new(),
            &[(predecessor_manifest, predecessor_bytes)],
            None,
        )
        .unwrap();
    let predecessor = decisions
        .create_committed_checkpoint(&predecessor_index)
        .await
        .unwrap();
    let retained = coordinator
        .predecessor_source_watermarks_until(
            Some(&predecessor),
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();

    let (mut manifest, _) = manifest(2, &deployment_id, None);
    manifest.source_names = vec!["orders".into()];
    let encoded = Bytes::from(checkpoint_manifest_bytes(&manifest).unwrap());

    let index = coordinator
        .build_committed_index(
            CheckpointAttempt::canonical(2),
            CheckpointScope::Local,
            None,
            Some(predecessor),
            &retained,
            &[(manifest, encoded)],
            None,
        )
        .unwrap();
    let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
    let restored = decisions
        .load_committed_checkpoint(&reference)
        .await
        .unwrap();

    assert!(restored.channel_progress.is_empty());
    assert_eq!(restored.source_watermarks.get("orders"), Some(&900));
    restored
        .validate_predecessor_index(&predecessor_index)
        .unwrap();
}

async fn save_prepared(
    coordinator: &mut CheckpointCoordinator,
    checkpoint_id: u64,
    deployment_id: &str,
) -> (CheckpointManifest, Bytes) {
    coordinator
        .begin_checkpoint_artifacts_until(
            CheckpointAttempt::canonical(checkpoint_id),
            None,
            None,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    let (manifest, payload) = manifest(checkpoint_id, deployment_id, None);
    let encoded = coordinator
        .store
        .save_checkpoint(&manifest, std::slice::from_ref(&payload))
        .await
        .unwrap();
    coordinator.prepared.insert(
        CheckpointAttempt::canonical(checkpoint_id),
        (Arc::new(manifest.clone()), encoded.clone()),
    );
    (manifest, encoded)
}

#[tokio::test]
async fn durable_abort_seals_only_its_exact_prepared_artifact_and_is_idempotent() {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (mut coordinator, _decisions, deployment_id) = coordinator_with_store(objects).await;
    let (aborted, _) = save_prepared(&mut coordinator, 1, &deployment_id).await;
    let (unrelated, unrelated_payload) = manifest(2, &deployment_id, None);
    coordinator
        .store
        .save_checkpoint(&unrelated, std::slice::from_ref(&unrelated_payload))
        .await
        .unwrap();

    let attempt = CheckpointAttempt::canonical(1);
    coordinator
        .abort_attempt_until(
            attempt,
            None,
            None,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    coordinator
        .abort_attempt_until(
            attempt,
            None,
            None,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();

    assert!(coordinator
        .store
        .load_manifest_for_participant(1, 1)
        .await
        .is_err());
    assert!(coordinator
        .store
        .load_node_data_ranges(
            aborted.node_data.chunk,
            aborted.node_data.object_length,
            &[],
        )
        .await
        .is_err());
    assert!(coordinator
        .store
        .load_manifest_for_participant(1, 2)
        .await
        .unwrap()
        .is_some());
    assert!(
        node_data_exists(
            coordinator.store.as_ref(),
            unrelated.node_data.chunk,
            unrelated.node_data.object_length,
        )
        .await
    );
}

#[tokio::test]
async fn recovery_aborts_and_seals_unresolved_candidate_index() {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (mut interrupted, decisions, deployment_id) =
        coordinator_with_store(Arc::clone(&objects)).await;
    let committed = interrupted
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(committed.success, "{committed:?}");
    let predecessor = interrupted.last_committed_ref.clone().unwrap();
    let (manifest, encoded) = save_prepared(&mut interrupted, 2, &deployment_id).await;
    let index = CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id,
        pipeline_identity: manifest.pipeline_identity.clone(),
        epoch: 2,
        checkpoint_id: 2,
        scope: CheckpointScope::Local,
        vnode_count: 1,
        assignment_fence: None,
        reassignment_portable: false,
        predecessor: Some(predecessor.clone()),
        participants: vec![CommittedParticipantRef::from_manifest(&manifest, &encoded).unwrap()],
        source_names: Vec::new(),
        source_offsets: BTreeMap::new(),
        channel_progress: Vec::new(),
        source_watermarks: BTreeMap::new(),
        checkpoint_watermark: None,
    };
    let candidate = decisions.create_committed_checkpoint(&index).await.unwrap();
    drop(interrupted);

    let (mut restarted, _, _) = coordinator_with_store(objects).await;
    assert_eq!(restarted.recover().await.unwrap().unwrap().epoch(), 1);
    assert_eq!(
        restarted.allocator.peek_epoch(),
        2,
        "recovery must restore the allocator to the committed successor epoch"
    );
    assert!(decisions
        .load_committed_checkpoint(&candidate)
        .await
        .is_err());
    assert!(decisions
        .load_committed_checkpoint(&predecessor)
        .await
        .is_ok());
    let head = decisions.checkpoint_decision_head().await.unwrap().unwrap();
    assert!(head.active_artifacts.is_none());
    assert!(head
        .latest_terminal
        .is_some_and(|outcome| !outcome.is_commit() && outcome.checkpoint_id == 2));
    assert!(restarted
        .store
        .load_manifest_for_participant(1, 2)
        .await
        .is_err());
    assert!(restarted
        .store
        .load_node_data_ranges(
            manifest.node_data.chunk,
            manifest.node_data.object_length,
            &[],
        )
        .await
        .is_err());
}

#[tokio::test]
async fn commit_winner_prevents_prepared_artifact_sealing() {
    use laminar_core::checkpoint_decision::CheckpointVerdict;

    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (mut coordinator, decisions, deployment_id) = coordinator_with_store(objects).await;
    let (manifest, encoded) = save_prepared(&mut coordinator, 1, &deployment_id).await;
    let participant = CommittedParticipantRef::from_manifest(&manifest, &encoded).unwrap();
    let index = CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id,
        pipeline_identity: manifest.pipeline_identity.clone(),
        epoch: 1,
        checkpoint_id: 1,
        scope: CheckpointScope::Local,
        vnode_count: 1,
        assignment_fence: None,
        reassignment_portable: false,
        predecessor: None,
        participants: vec![participant],
        source_names: Vec::new(),
        source_offsets: BTreeMap::new(),
        channel_progress: Vec::new(),
        source_watermarks: BTreeMap::new(),
        checkpoint_watermark: None,
    };
    let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
    decisions
        .record_outcome(
            1,
            1,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            Some(reference),
        )
        .await
        .unwrap();

    assert!(coordinator
        .abort_attempt_until(
            CheckpointAttempt::canonical(1),
            None,
            None,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .is_err());
    assert!(coordinator
        .store
        .load_manifest_for_participant(1, 1)
        .await
        .unwrap()
        .is_some());
    assert!(
        node_data_exists(
            coordinator.store.as_ref(),
            manifest.node_data.chunk,
            manifest.node_data.object_length,
        )
        .await
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn follower_assignment_authority_validation_is_bounded_by_attempt_deadline() {
    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let blocked = Arc::new(CommitThenIoStore {
        inner,
        lose_put_ack: std::sync::atomic::AtomicBool::new(false),
        path_suffix: "/manifest.json",
        block_get: std::sync::atomic::AtomicBool::new(false),
        deny_list: std::sync::atomic::AtomicBool::new(false),
    });
    let authority_objects: Arc<dyn object_store::ObjectStore> = blocked.clone();
    let authority = Arc::new(LeaderLeaseStore::new(authority_objects, 1_000));
    let leader_boot = uuid::Uuid::from_u128(1);
    let follower_boot = uuid::Uuid::from_u128(2);
    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: leader_boot,
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty checkpoint authority must grant its first leader term");
    };
    let proof = lease.proof();
    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: leader_boot,
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: follower_boot,
            },
        ],
    )
    .unwrap();
    let local_kv = Arc::new(InMemoryKv::new(NodeId(2)));
    let control_kv: Arc<dyn ClusterKv> = local_kv;
    let (_members_tx, members_rx) = tokio::sync::watch::channel(vec![NodeInfo {
        id: NodeId(1),
        name: "leader".into(),
        rpc_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    }]);
    let controller = ClusterController::new_with_recovery_incarnation(
        NodeId(2),
        Arc::clone(&control_kv),
        control_kv,
        None,
        members_rx,
        follower_boot,
    );
    controller.set_leader_lease_store(authority);
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));

    blocked
        .block_get
        .store(true, std::sync::atomic::Ordering::Release);
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_secs(5);
    let validation = CheckpointCoordinator::certify_follower_assignment_until(
        &controller,
        &fence,
        &proof,
        deadline,
        "follower Prepare",
    );
    tokio::pin!(validation);
    tokio::select! {
        result = &mut validation => panic!("authority validation completed before its deadline: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    tokio::time::advance(Duration::from_secs(5)).await;
    let error = validation
        .await
        .expect_err("a stalled authority read must expire at the attempt deadline");
    assert_eq!(tokio::time::Instant::now(), deadline);
    assert!(error
        .to_string()
        .contains("follower Prepare authority validation timed out"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_leader_durably_admits_sink_epoch_before_opening_local_gate() {
    use laminar_connectors::connector::{
        SinkConsistency, SinkContract, SinkInputMode, SinkTopology,
    };

    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&objects)));
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 1_000));
    let boot = uuid::Uuid::from_u128(1);
    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot,
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty checkpoint authority must grant its first leader term");
    };
    let proof = lease.proof();
    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[1],
        vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: boot,
        }],
    )
    .unwrap();
    let control_kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(NodeId(1)));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        NodeId(1),
        Arc::clone(&control_kv),
        control_kv,
        None,
        members_rx,
        boot,
    ));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(10))))
        .unwrap();
    let (_lease_tx, lease_rx) = tokio::sync::watch::channel(Some(lease));
    controller
        .set_leader_lease_watch(
            lease_rx,
            owner,
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(10))),
        )
        .unwrap();
    controller.set_leader_lease_store(Arc::clone(&authority));
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));

    let store = ObjectStoreCheckpointStore::new(Arc::clone(&objects), "leader-sink-epoch")
        .with_participant_id(1);
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    coordinator
        .bind_durable_decision_store(decisions)
        .await
        .unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();
    coordinator.set_assignment_version(fence.assignment_version);
    coordinator.set_cluster_controller(controller);
    let sink = AmbiguousFollowerSink {
        rollbacks: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        schema: Arc::new(arrow::datatypes::Schema::empty()),
        expected_admission: Some((
            Arc::clone(&authority),
            CheckpointAttempt::canonical(1),
            proof.clone(),
        )),
        expected_intent: Some((
            Arc::clone(&objects),
            object_store::path::Path::from(
                "leader-sink-epoch/nodes/1/checkpoints/00000000000000000001/manifest.json",
            ),
        )),
    };
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let sink_handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "probe".into(),
        sink_id: Arc::from("probe"),
        connector: Box::new(sink),
        contract: SinkContract::new(
            SinkConsistency::CheckpointCommittable,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(1),
        event_tx,
        terminal_tasks: None,
        process_authority: None,
    });
    coordinator.register_sink("probe", sink_handle.clone());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    coordinator
        .begin_sink_epoch_until(deadline, SinkEpochPublication::DeferredToTail)
        .await
        .unwrap();
    assert!(sink_handle.open_epoch_admission(1).is_err());
    let admission = sink_handle.current_begun_epoch_admission().unwrap();
    let publisher = tokio::spawn({
        let sink_handle = sink_handle.clone();
        async move {
            tokio::task::yield_now().await;
            sink_handle.publish_open_epoch(admission)
        }
    });
    coordinator
        .ensure_assignment_sink_epoch_until(deadline)
        .await
        .unwrap();
    publisher.await.unwrap().unwrap();
    let (inventory, admitted_proof) = authority
        .cluster_checkpoint_artifact_admission()
        .await
        .unwrap()
        .expect("leader must publish durable attempt authority");
    assert_eq!(inventory.attempt, CheckpointAttempt::canonical(1));
    assert_eq!(inventory.assignment_fence.as_ref(), Some(&fence));
    assert_eq!(admitted_proof, proof);
    assert!(sink_handle.open_epoch_admission(1).is_ok());
    sink_handle.close().await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_ownerless_worker_keeps_initial_exact_sink_epoch_closed() {
    use laminar_connectors::connector::{
        SinkConsistency, SinkContract, SinkInputMode, SinkTopology,
    };

    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let owner_boot = uuid::Uuid::from_u128(1);
    let worker_boot = uuid::Uuid::from_u128(2);
    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[1],
        vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: owner_boot,
        }],
    )
    .unwrap();
    let control_kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(NodeId(2)));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(vec![NodeInfo {
        id: NodeId(1),
        name: "owner".into(),
        rpc_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    }]);
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        NodeId(2),
        Arc::clone(&control_kv),
        control_kv,
        None,
        members_rx,
        worker_boot,
    ));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(10))))
        .unwrap();

    let store = ObjectStoreCheckpointStore::new(objects, "ownerless-initial-sink-epoch")
        .with_key_group_count(KeyGroupCount::try_from(1_u16).unwrap())
        .with_participant_id(2);
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    coordinator.set_assignment_version(fence.assignment_version);
    coordinator.set_vnode_set(Vec::new());
    coordinator.set_cluster_controller(Arc::clone(&controller));
    let sink = AmbiguousFollowerSink {
        rollbacks: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        schema: Arc::new(arrow::datatypes::Schema::empty()),
        expected_admission: None,
        expected_intent: None,
    };
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let sink_handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "probe".into(),
        sink_id: Arc::from("probe"),
        connector: Box::new(sink),
        contract: SinkContract::new(
            SinkConsistency::CheckpointCommittable,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(1),
        event_tx,
        terminal_tasks: None,
        process_authority: None,
    });
    coordinator.register_sink("probe", sink_handle.clone());

    assert!(coordinator.initial_sink_epoch_required().unwrap());
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    assert_eq!(
        controller.checkpoint_assignment_fence(fence.assignment_version),
        Some(fence)
    );
    assert!(coordinator.certified_idle_process().unwrap());
    coordinator.begin_initial_epoch().await.unwrap();
    assert!(sink_handle.open_epoch_admission(1).is_err());

    coordinator.set_vnode_set(vec![0]);
    assert!(coordinator.certified_idle_process().is_err());
    let error = coordinator.begin_initial_epoch().await.unwrap_err();
    assert!(
        error.to_string().contains("excludes participant 2"),
        "{error}"
    );
    assert!(sink_handle.open_epoch_admission(1).is_err());
    sink_handle.close().await.unwrap();
}

#[cfg(feature = "cluster")]
#[test]
fn cluster_recovery_reuses_the_checkpoint_bound_external_fence() {
    use laminar_core::checkpoint::{LeaderProof, LeaderProofOwner};

    let proof = |node_id, fencing_token| LeaderProof {
        owner: LeaderProofOwner {
            node_id,
            boot_id: uuid::Uuid::from_u128(u128::from(node_id)),
            process_term: 1,
        },
        fencing_token,
    };
    let checkpoint = proof(1, 7);
    let successor = proof(2, 9);

    assert_eq!(
        recovery_sink_fence(Some(&checkpoint), Some(&successor)).unwrap(),
        Some(checkpoint.fencing_token),
        "the current leader designates publication, but the committed cut retains its exact token"
    );
    assert_eq!(
        recovery_sink_fence(Some(&checkpoint), None).unwrap(),
        None,
        "a follower must not publish the external cut"
    );
    let regressed = proof(2, 6);
    assert!(recovery_sink_fence(Some(&checkpoint), Some(&regressed))
        .unwrap_err()
        .to_string()
        .contains("regressed"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_manifest_ack_loss_preserves_prepared_sink_until_authoritative_commit() {
    use laminar_connectors::connector::{
        SinkConsistency, SinkContract, SinkInputMode, SinkTopology,
    };

    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(CommitThenIoStore {
        inner: Arc::clone(&objects),
        lose_put_ack: std::sync::atomic::AtomicBool::new(false),
        path_suffix: "/manifest.json",
        block_get: std::sync::atomic::AtomicBool::new(false),
        deny_list: std::sync::atomic::AtomicBool::new(false),
    });
    let checkpoint_objects: Arc<dyn object_store::ObjectStore> = flaky.clone();
    let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&objects)));
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 1_000));
    let leader_boot = uuid::Uuid::from_u128(1);
    let follower_boot = uuid::Uuid::from_u128(2);
    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: leader_boot,
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty checkpoint authority must grant its first leader term");
    };
    let proof = lease.proof();
    let key_groups = KeyGroupCount::try_from(2_u16).unwrap();
    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: leader_boot,
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: follower_boot,
            },
        ],
    )
    .unwrap();

    let local_kv = Arc::new(InMemoryKv::new(NodeId(2)));
    let control_kv: Arc<dyn ClusterKv> = local_kv.clone();
    let (_members_tx, members_rx) = tokio::sync::watch::channel(vec![NodeInfo {
        id: NodeId(1),
        name: "leader".into(),
        rpc_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    }]);
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        NodeId(2),
        Arc::clone(&control_kv),
        control_kv,
        None,
        members_rx,
        follower_boot,
    ));
    controller.set_leader_lease_store(Arc::clone(&authority));
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    assert_eq!(
        controller
            .checkpoint_assignment_fence_for_leader(fence.assignment_version, &proof)
            .await,
        Some(fence.clone()),
        "fixture must certify the follower's exact leader and assignment fence"
    );

    let prefix = "follower-manifest-ack-loss";
    let store = ObjectStoreCheckpointStore::new(checkpoint_objects, prefix)
        .with_key_group_count(key_groups)
        .with_participant_id(2);
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    coordinator
        .bind_durable_decision_store(Arc::clone(&decisions))
        .await
        .unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();
    coordinator.set_assignment_version(fence.assignment_version);
    coordinator.set_vnode_set(vec![1]);
    coordinator.set_cluster_controller(Arc::clone(&controller));

    let rollbacks = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let sink = AmbiguousFollowerSink {
        rollbacks: Arc::clone(&rollbacks),
        schema: Arc::new(arrow::datatypes::Schema::empty()),
        expected_admission: None,
        expected_intent: None,
    };
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let sink_handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "probe".into(),
        sink_id: Arc::from("probe"),
        connector: Box::new(sink),
        contract: SinkContract::new(
            SinkConsistency::CheckpointCommittable,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(1),
        event_tx,
        terminal_tasks: None,
        process_authority: None,
    });
    coordinator.register_sink("probe", sink_handle.clone());

    let attempt = CheckpointAttempt::canonical(7);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    coordinator
        .begin_checkpoint_artifacts_until(attempt, Some(fence.clone()), Some(&proof), deadline)
        .await
        .unwrap();
    coordinator.begin_initial_epoch().await.unwrap();
    assert_eq!(coordinator.allocator.peek_epoch(), attempt.epoch);
    sink_handle
        .seal_epoch_for_protocol_until(attempt.epoch, deadline)
        .await
        .unwrap();
    controller
        .ack_barrier(&BarrierAck {
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            assignment_digest: Some(fence.digest()),
            flags: 0,
            disposition: BarrierAckDisposition::Captured,
            error: None,
            watermark: CheckpointWatermark::Uninitialized,
        })
        .await
        .unwrap();

    flaky
        .lose_put_ack
        .store(true, std::sync::atomic::Ordering::Release);
    let outcome = coordinator
        .follower_prepare_acked_until(
            CheckpointRequest {
                assignment_fence: Some(fence.clone()),
                reassignment_portable: true,
                ..CheckpointRequest::default()
            },
            proof.clone(),
            attempt.epoch,
            attempt.checkpoint_id,
            deadline,
        )
        .await
        .unwrap();
    assert_eq!(outcome, FollowerPrepareOutcome::InDoubt);
    assert_eq!(coordinator.phase, CheckpointPhase::Idle);
    assert_eq!(rollbacks.load(std::sync::atomic::Ordering::Acquire), 0);

    let cached_ack = local_kv
        .read_from_checked(NodeId(2), ACK_KEY)
        .await
        .unwrap()
        .expect("captured acknowledgement must remain cached");
    let cached_ack: BarrierAck = serde_json::from_str(&cached_ack).unwrap();
    assert_eq!(cached_ack.disposition, BarrierAckDisposition::Captured);
    assert!(cached_ack.error.is_none());

    let (follower_manifest, follower_manifest_bytes) = coordinator
        .prepared
        .get(&attempt)
        .cloned()
        .expect("acknowledgement loss must retain the exact prepared candidate");
    assert_eq!(
        coordinator
            .store
            .load_manifest_for_participant(2, attempt.checkpoint_id)
            .await
            .unwrap()
            .as_ref(),
        Some(follower_manifest.as_ref()),
        "the manifest Create succeeded even though its acknowledgement was lost"
    );

    let leader_store = ObjectStoreCheckpointStore::new(Arc::clone(&objects), prefix)
        .with_key_group_count(key_groups)
        .with_participant_id(1);
    let (mut leader_manifest, leader_payload) = cluster_manifest(
        attempt.checkpoint_id,
        1,
        0,
        &deployment_id,
        &fence,
        key_groups,
    );
    leader_manifest.sink_names = vec!["probe".into()];
    leader_manifest.prepared_sinks = vec![PreparedSinkDescriptor {
        sink_name: "probe".into(),
        format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
        payload: None,
        sha256: checkpoint_descriptor_sha256(None),
    }];
    let leader_manifest_bytes = leader_store
        .save_checkpoint(&leader_manifest, std::slice::from_ref(&leader_payload))
        .await
        .unwrap();
    let manifests = vec![
        (leader_manifest, leader_manifest_bytes),
        ((*follower_manifest).clone(), follower_manifest_bytes),
    ];
    let index = coordinator
        .build_committed_index(
            attempt,
            CheckpointScope::Cluster,
            Some(fence.clone()),
            None,
            &BTreeMap::new(),
            &manifests,
            None,
        )
        .unwrap();
    let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
    authority
        .record_cluster_outcome(
            &proof,
            attempt.epoch,
            attempt.checkpoint_id,
            fence.clone(),
            laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
            Some(reference.clone()),
        )
        .await
        .unwrap();
    let successor = CheckpointArtifactInventory {
        deployment_id,
        pipeline_identity: PipelineIdentity::empty(),
        attempt: CheckpointAttempt::canonical(8),
        assignment_fence: Some(fence),
        sink_artifact_intent_protocol: true,
    };
    authority
        .begin_cluster_checkpoint_artifacts(&proof, successor.clone())
        .await
        .unwrap();

    assert!(coordinator
        .follower_finish_deferred(
            attempt.epoch,
            attempt.checkpoint_id,
            true,
            Instant::now(),
            false,
        )
        .await
        .unwrap());
    assert_eq!(coordinator.allocator.peek_epoch(), successor.attempt.epoch);
    assert!(!coordinator.prepared.contains_key(&attempt));
    assert_eq!(coordinator.last_committed_ref(), Some(&reference));
    assert_eq!(
        rollbacks.load(std::sync::atomic::Ordering::Acquire),
        0,
        "a durable Commit must never follow unilateral follower rollback"
    );
    sink_handle.close().await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_settlement_resumes_exact_seals_and_rejects_a_genesis_fork() {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&objects)));
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 1_000));
    let leader_boot = uuid::Uuid::from_u128(1);
    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: leader_boot,
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty cluster authority must grant its first leader term");
    };
    let proof = lease.proof();
    let key_groups = KeyGroupCount::try_from(2_u16).unwrap();
    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: leader_boot,
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
        ],
    )
    .unwrap();

    let control_kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(NodeId(1)));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new(
        NodeId(1),
        control_kv,
        None,
        members_rx,
    ));
    controller.set_leader_lease_store(Arc::clone(&authority));

    let prefix = "cluster-aborted-artifacts";
    let store = ObjectStoreCheckpointStore::new(Arc::clone(&objects), prefix)
        .with_key_group_count(key_groups)
        .with_participant_id(1);
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    coordinator
        .bind_durable_decision_store(Arc::clone(&decisions))
        .await
        .unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();
    coordinator.set_cluster_controller(Arc::clone(&controller));
    let cleanup_calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (cleanup_handle, _cleanup_events) = register_cluster_cleanup_probe(
        &mut coordinator,
        Arc::clone(&cleanup_calls),
        proof.fencing_token,
    );

    let nonportable_request = CheckpointRequest {
        assignment_fence: Some(fence.clone()),
        ..CheckpointRequest::default()
    };
    let error = coordinator
        .validate_request(&nonportable_request)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cluster checkpoint requires a vnode-reassignment-portable capture"),
        "{error}"
    );

    let predecessor_attempt = CheckpointAttempt::canonical(1);
    let predecessor_inventory = CheckpointArtifactInventory {
        deployment_id: deployment_id.clone(),
        pipeline_identity: PipelineIdentity::empty(),
        attempt: predecessor_attempt,
        assignment_fence: Some(fence.clone()),
        sink_artifact_intent_protocol: true,
    };
    authority
        .begin_cluster_checkpoint_artifacts(&proof, predecessor_inventory)
        .await
        .unwrap();
    let predecessor_manifests = save_cluster_manifests(
        Arc::clone(&objects),
        prefix,
        1,
        &deployment_id,
        &fence,
        key_groups,
        Some("probe"),
    )
    .await;
    let mut nonportable_manifests = predecessor_manifests.clone();
    nonportable_manifests[1].0.reassignment_portable = false;
    nonportable_manifests[1].1 =
        Bytes::from(checkpoint_manifest_bytes(&nonportable_manifests[1].0).unwrap());
    let error = coordinator
        .build_committed_index(
            predecessor_attempt,
            CheckpointScope::Cluster,
            Some(fence.clone()),
            None,
            &BTreeMap::new(),
            &nonportable_manifests,
            None,
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("must be proven portable across vnode reassignment"),
        "{error}"
    );
    let predecessor_index = coordinator
        .build_committed_index(
            predecessor_attempt,
            CheckpointScope::Cluster,
            Some(fence.clone()),
            None,
            &BTreeMap::new(),
            &predecessor_manifests,
            None,
        )
        .unwrap();
    assert!(predecessor_index.reassignment_portable);
    let predecessor = decisions
        .create_committed_checkpoint(&predecessor_index)
        .await
        .unwrap();
    authority
        .record_cluster_outcome(
            &proof,
            1,
            1,
            fence.clone(),
            laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
            Some(predecessor.clone()),
        )
        .await
        .unwrap();
    assert!(coordinator.last_committed_ref.is_none());
    assert_eq!(
        coordinator
            .authoritative_committed_predecessor_until(
                CheckpointScope::Cluster,
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap(),
        Some(predecessor.clone())
    );

    let attempt = CheckpointAttempt::canonical(2);
    let inventory = CheckpointArtifactInventory {
        deployment_id: deployment_id.clone(),
        pipeline_identity: PipelineIdentity::empty(),
        attempt,
        assignment_fence: Some(fence.clone()),
        sink_artifact_intent_protocol: true,
    };
    authority
        .begin_cluster_checkpoint_artifacts(&proof, inventory.clone())
        .await
        .unwrap();
    let manifests = save_cluster_manifests(
        Arc::clone(&objects),
        prefix,
        2,
        &deployment_id,
        &fence,
        key_groups,
        Some("probe"),
    )
    .await;
    let candidate_index = coordinator
        .build_committed_index(
            attempt,
            CheckpointScope::Cluster,
            Some(fence.clone()),
            Some(predecessor.clone()),
            &predecessor_index.source_watermarks,
            &manifests,
            None,
        )
        .unwrap();
    let candidate = decisions
        .create_committed_checkpoint(&candidate_index)
        .await
        .unwrap();
    authority
        .record_cluster_outcome(
            &proof,
            2,
            2,
            fence,
            laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();

    coordinator.prepared.insert(
        attempt,
        (Arc::new(manifests[0].0.clone()), manifests[0].1.clone()),
    );
    coordinator.failure_requires_recovery = true;
    coordinator.local_watermark = CheckpointWatermark::Active(42);
    coordinator.allocator.advance_epoch_to(9);
    let allocator_epoch = coordinator.allocator.peek_epoch();
    let genesis_error = coordinator.recover_to_epoch(0).await.unwrap_err();
    assert!(
        genesis_error
            .to_string()
            .contains("checkpoint artifacts remain"),
        "{genesis_error}"
    );
    assert!(coordinator.prepared.contains_key(&attempt));
    assert!(coordinator.failure_requires_recovery);
    assert_eq!(coordinator.local_watermark, CheckpointWatermark::Active(42));
    assert_eq!(coordinator.allocator.peek_epoch(), allocator_epoch);
    assert_eq!(cleanup_calls.load(std::sync::atomic::Ordering::Acquire), 0);

    let first_chunk = manifests[0].0.node_data.chunk;
    let first_identity = checkpoint_artifact_identity_sha256(&inventory, first_chunk).unwrap();
    let sealer = ObjectStoreCheckpointStore::new(Arc::clone(&objects), prefix)
        .with_key_group_count(key_groups)
        .with_participant_id(1);
    assert!(sealer
        .seal_aborted_manifest(
            first_chunk,
            &first_identity,
            inventory.sink_artifact_intent_protocol,
        )
        .await
        .unwrap()
        .original_manifest
        .is_some());

    cleanup_handle.close().await.unwrap();

    assert!(coordinator
        .settle_cluster_checkpoint_artifacts_until(
            &proof,
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap());
    assert_eq!(cleanup_calls.load(std::sync::atomic::Ordering::Acquire), 1);
    assert!(authority
        .cluster_checkpoint_artifacts()
        .await
        .unwrap()
        .is_none());
    assert!(decisions
        .load_committed_checkpoint(&candidate)
        .await
        .is_err());
    assert_eq!(
        decisions
            .load_committed_checkpoint(&predecessor)
            .await
            .unwrap(),
        predecessor_index
    );
    for (manifest, _) in &manifests {
        assert!(coordinator
            .store
            .load_manifest_for_participant(manifest.participant_id, attempt.checkpoint_id)
            .await
            .is_err());
        assert!(coordinator
            .store
            .load_node_data_ranges(
                manifest.node_data.chunk,
                manifest.node_data.object_length,
                &[],
            )
            .await
            .is_err());
    }
    assert!(!coordinator
        .settle_cluster_checkpoint_artifacts_until(
            &proof,
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap());

    coordinator.prepared.insert(
        attempt,
        (Arc::new(manifests[0].0.clone()), manifests[0].1.clone()),
    );
    coordinator.last_committed_manifest = Some(Arc::new(predecessor_manifests[0].0.clone()));
    coordinator.last_committed_ref = Some(predecessor);
    coordinator.failure_requires_recovery = true;
    coordinator.local_watermark = CheckpointWatermark::Active(42);
    let genesis_error = coordinator.recover_to_epoch(0).await.unwrap_err();
    assert!(
        genesis_error
            .to_string()
            .contains("cannot replace authoritative committed checkpoint"),
        "{genesis_error}"
    );
    assert!(coordinator.prepared.contains_key(&attempt));
    assert!(coordinator.last_committed_manifest.is_some());
    assert!(coordinator.last_committed_ref.is_some());
    assert!(coordinator.failure_requires_recovery);
    assert_eq!(coordinator.local_watermark, CheckpointWatermark::Active(42));
    assert_eq!(coordinator.allocator.peek_epoch(), allocator_epoch);
}
