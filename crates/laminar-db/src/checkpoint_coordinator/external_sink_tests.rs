use std::sync::atomic::{AtomicUsize, Ordering};
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
    checkpoint_descriptor_sha256, CheckpointAttempt, CheckpointManifest,
    ObjectStoreCheckpointStore, PipelineIdentity, PreparedSinkDescriptor,
    PREPARED_SINK_DESCRIPTOR_VERSION,
};
use laminar_core::state::KeyGroupCount;
use object_store::memory::InMemory;

use super::*;

struct BarrierCommitSink {
    barrier: Arc<tokio::sync::Barrier>,
    commits: Arc<AtomicUsize>,
    schema: SchemaRef,
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
