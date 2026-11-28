use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, CoordinatedCommitBatch, CoordinatedCommitContext,
    CoordinatedCommitCursor, CoordinatedCommitNamespace, CoordinatedCommitter, SinkConnector,
    SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::checkpoint::{
    checkpoint_descriptor_sha256, CheckpointAttempt, CheckpointManifest, CommittedCheckpointRef,
    ObjectStoreCheckpointStore, PipelineIdentity, PreparedSinkDescriptor,
    PIPELINE_IDENTITY_VERSION, PREPARED_SINK_DESCRIPTOR_VERSION,
};
use laminar_core::checkpoint_decision::CheckpointDecisionStore;
use laminar_core::state::KeyGroupCount;
use object_store::memory::InMemory;
use parking_lot::Mutex;

use super::*;

/// One cursor per namespace, as an external target keeps one per commit key.
#[derive(Clone, Default)]
struct Cursors(Arc<Mutex<HashMap<PipelineIdentity, CoordinatedCommitCursor>>>);

impl Cursors {
    fn set(&self, identity: &PipelineIdentity, checkpoint_id: u64) {
        self.0.lock().insert(
            identity.clone(),
            CoordinatedCommitCursor {
                checkpoint_id,
                fencing_token: 1,
            },
        );
    }

    fn get(&self, identity: &PipelineIdentity) -> Option<CoordinatedCommitCursor> {
        self.0.lock().get(identity).copied()
    }
}

struct NamespacedSink {
    cursors: Cursors,
    published: Arc<Mutex<Vec<CoordinatedCommitBatch>>>,
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl SinkConnector for NamespacedSink {
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
impl CoordinatedCommitter for NamespacedSink {
    async fn commit_aggregated(
        &self,
        batch: CoordinatedCommitBatch,
        _context: CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        batch
            .validate_observed_cursor(self.cursors.get(&batch.namespace.pipeline_identity))
            .map_err(ConnectorError::TransactionError)?;
        self.cursors.set(
            &batch.namespace.pipeline_identity,
            batch.target.checkpoint_id,
        );
        self.published.lock().push(batch);
        Ok(())
    }

    async fn committed_cursor(
        &self,
        namespace: &CoordinatedCommitNamespace,
    ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
        Ok(self.cursors.get(&namespace.pipeline_identity))
    }
}

fn identity(digit: char) -> PipelineIdentity {
    PipelineIdentity {
        canonical_version: PIPELINE_IDENTITY_VERSION,
        sha256: std::iter::repeat_n(digit, 64).collect(),
    }
}

struct Migrated {
    target: CheckpointCoordinator,
    handle: crate::sink_task::SinkTaskHandle,
    cursors: Cursors,
    published: Arc<Mutex<Vec<CoordinatedCommitBatch>>>,
    decisions: Arc<CheckpointDecisionStore>,
    objects: Arc<dyn object_store::ObjectStore>,
}

fn no_rows_staged_for(sink: &str) -> PreparedSinkDescriptor {
    PreparedSinkDescriptor {
        sink_name: sink.into(),
        format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
        payload: None,
        sha256: checkpoint_descriptor_sha256(None),
    }
}

fn checkpoint_store(objects: &Arc<dyn object_store::ObjectStore>) -> ObjectStoreCheckpointStore {
    ObjectStoreCheckpointStore::new(Arc::clone(objects), "migrated-sink")
        .with_key_group_count(KeyGroupCount::try_from(1_u16).unwrap())
}

async fn coordinator(
    objects: &Arc<dyn object_store::ObjectStore>,
    decisions: &Arc<CheckpointDecisionStore>,
    pipeline: PipelineIdentity,
) -> CheckpointCoordinator {
    let mut coordinator = CheckpointCoordinator::new(
        CheckpointConfig::default(),
        Box::new(checkpoint_store(objects)),
    )
    .unwrap();
    coordinator
        .bind_durable_decision_store(Arc::clone(decisions))
        .await
        .unwrap();
    coordinator.bind_pipeline_identity(pipeline).unwrap();
    coordinator
}

/// A target coordinator whose committable sink keeps one cursor per pipeline identity.
async fn migrated() -> Migrated {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&objects)));
    let mut target = coordinator(&objects, &decisions, identity('b')).await;
    let cursors = Cursors::default();
    let published = Arc::new(Mutex::new(Vec::new()));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "sink".into(),
        sink_id: Arc::from("sink"),
        connector: Box::new(NamespacedSink {
            cursors: cursors.clone(),
            published: Arc::clone(&published),
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
    target.register_sink("sink", handle.clone());
    Migrated {
        target,
        handle,
        cursors,
        published,
        decisions,
        objects,
    }
}

impl Migrated {
    /// The committed checkpoint `checkpoint_id` of `pipeline`, as the target's predecessor, whose
    /// one participant prepared a descriptor for each of `prepared`.
    async fn committed(
        &self,
        pipeline: PipelineIdentity,
        checkpoint_id: u64,
        prepared: &[&str],
    ) -> CommittedCheckpointRef {
        let owner = coordinator(&self.objects, &self.decisions, pipeline.clone()).await;
        let mut manifest = CheckpointManifest::new_with_key_group_count(
            checkpoint_id,
            checkpoint_id,
            KeyGroupCount::try_from(1_u16).unwrap(),
        );
        manifest.deployment_id = owner.expected_deployment_id().unwrap().to_owned();
        manifest.pipeline_identity = pipeline;
        manifest.sink_names = prepared.iter().map(|&sink| sink.to_owned()).collect();
        manifest.prepared_sinks = prepared
            .iter()
            .map(|&sink| no_rows_staged_for(sink))
            .collect();
        let encoded = checkpoint_store(&self.objects)
            .save_checkpoint(&manifest, &[])
            .await
            .unwrap();
        let index = owner
            .build_committed_index(
                CheckpointAttempt::canonical(checkpoint_id),
                CheckpointScope::Local,
                None,
                None,
                &CommittedFrontiers::default(),
                &[(manifest, encoded)],
                None,
            )
            .unwrap();
        self.decisions
            .create_committed_checkpoint(&index)
            .await
            .unwrap()
    }

    /// Publish checkpoint 4, whose one participant staged no rows.
    async fn publish(&self, predecessor: &CommittedCheckpointRef) -> Result<(), DbError> {
        let mut manifest = CheckpointManifest::new_with_key_group_count(
            4,
            4,
            KeyGroupCount::try_from(1_u16).unwrap(),
        );
        manifest.bind_participant(1);
        manifest.prepared_sinks = vec![no_rows_staged_for("sink")];
        self.target
            .commit_external_sinks_until(
                CheckpointAttempt::canonical(4),
                &[&manifest],
                1,
                Some(predecessor),
                tokio::time::Instant::now() + Duration::from_secs(5),
            )
            .await
    }
}

#[tokio::test]
async fn a_migrated_sink_publishes_its_first_cut_after_the_parent_published_the_migration_cut() {
    let migrated = migrated().await;
    let cut = migrated.committed(identity('a'), 3, &["sink"]).await;
    migrated.cursors.set(&identity('a'), 3);

    migrated.publish(&cut).await.unwrap();
    migrated.publish(&cut).await.unwrap();

    let published = std::mem::take(&mut *migrated.published.lock());
    assert_eq!(
        published.len(),
        1,
        "a re-driven cut publishes nothing again"
    );
    assert_eq!(published[0].namespace.pipeline_identity, identity('b'));
    assert_eq!(published[0].expected_predecessor.checkpoint_id, 0);
    assert_eq!(published[0].target, CheckpointAttempt::canonical(4));
    assert_eq!(
        migrated.cursors.get(&identity('a')).unwrap().checkpoint_id,
        3
    );
    migrated.handle.close().await.unwrap();
}

#[tokio::test]
async fn a_sink_migrated_back_to_an_earlier_identity_publishes_after_its_old_cursor() {
    let migrated = migrated().await;
    migrated.cursors.set(&identity('b'), 1);
    let cut = migrated.committed(identity('a'), 3, &["sink"]).await;
    migrated.cursors.set(&identity('a'), 3);

    migrated.publish(&cut).await.unwrap();

    let published = std::mem::take(&mut *migrated.published.lock());
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].namespace.pipeline_identity, identity('b'));
    assert_eq!(published[0].expected_predecessor.checkpoint_id, 1);
    migrated.handle.close().await.unwrap();
}

#[tokio::test]
async fn a_sink_the_migration_added_publishes_its_first_cut_from_the_zero_cursor() {
    let migrated = migrated().await;
    let cut = migrated.committed(identity('a'), 3, &[]).await;

    migrated.publish(&cut).await.unwrap();

    let published = std::mem::take(&mut *migrated.published.lock());
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].namespace.pipeline_identity, identity('b'));
    assert_eq!(published[0].expected_predecessor.checkpoint_id, 0);
    assert!(migrated.cursors.get(&identity('a')).is_none());
    migrated.handle.close().await.unwrap();
}

#[tokio::test]
async fn a_migrated_sink_refuses_a_cut_its_parent_never_published() {
    let migrated = migrated().await;
    let cut = migrated.committed(identity('a'), 3, &["sink"]).await;

    let unpublished = migrated.publish(&cut).await.unwrap_err().to_string();
    assert!(
        unpublished.contains("has no external cursor for committed predecessor 3"),
        "{unpublished}"
    );
    migrated.cursors.set(&identity('a'), 2);
    let trailing = migrated.publish(&cut).await.unwrap_err().to_string();
    assert!(
        trailing.contains("parent-topology cursor 2 is not the committed migration cut 3"),
        "{trailing}"
    );

    let same_pipeline = migrated.committed(identity('b'), 2, &["sink"]).await;
    let missing = migrated
        .publish(&same_pipeline)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        missing.contains("has no external cursor for committed predecessor 2"),
        "a predecessor of the same pipeline must be in the sink's own namespace: {missing}"
    );
    assert!(migrated.published.lock().is_empty());
    migrated.handle.close().await.unwrap();
}
