//! Decoupled designated committer for `coordinated_commit` sinks.
//!
//! Runs off the checkpoint barrier path: a pass reads each writer's commit
//! descriptor for every sealed-but-uncommitted epoch from the state backend and
//! drives one external commit per sink. Sequential and ascending per sink; a
//! failed commit leaves the cursor so the next pass retries.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use laminar_core::state::StateBackend;

use crate::error::DbError;
use crate::sink_task::SinkTaskHandle;

/// Drives aggregated commits for coordinated-commit sinks on the leader.
pub(crate) struct CoordinatedCommitter {
    backend: Arc<dyn StateBackend>,
    sinks: Vec<(String, SinkTaskHandle)>,
    committed_through: FxHashMap<String, u64>,
}

impl CoordinatedCommitter {
    pub(crate) fn new(
        backend: Arc<dyn StateBackend>,
        sinks: Vec<(String, SinkTaskHandle)>,
    ) -> Self {
        Self {
            backend,
            sinks,
            committed_through: FxHashMap::default(),
        }
    }

    /// Commit every sealed-but-uncommitted epoch. Per-sink isolated: a sink that
    /// fails stops at its cursor while others proceed; the first error is returned.
    pub(crate) async fn commit_ready(&mut self) -> Result<(), DbError> {
        let high = self
            .backend
            .latest_committed_epoch()
            .await
            .map_err(|e| DbError::Checkpoint(format!("committer: read sealed epoch: {e}")))?
            .unwrap_or(0);

        let mut first_err: Option<DbError> = None;
        for (name, handle) in &self.sinks {
            let suffix = format!("/sink={name}");
            let mut cursor = self.committed_through.get(name).copied().unwrap_or(0);
            while cursor < high {
                let epoch = cursor + 1;
                // Abandoned epochs leave partial descriptors but no seal; their
                // data reprocesses into a later epoch, so skip them.
                match self.commit_epoch(handle, epoch, &suffix).await {
                    Ok(()) => {}
                    Err(e) => {
                        first_err.get_or_insert(e);
                        break;
                    }
                }
                cursor = epoch;
                self.committed_through.insert(name.clone(), cursor);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Commit one sealed epoch's descriptors for a sink. Unsealed epochs are a
    /// no-op (skipped). Errors leave the caller's cursor unadvanced.
    async fn commit_epoch(
        &self,
        handle: &SinkTaskHandle,
        epoch: u64,
        sink_suffix: &str,
    ) -> Result<(), DbError> {
        if !self
            .backend
            .is_epoch_sealed(epoch)
            .await
            .map_err(|e| DbError::Checkpoint(format!("committer: seal check epoch {epoch}: {e}")))?
        {
            return Ok(());
        }
        let descriptors: Vec<Vec<u8>> = self
            .backend
            .read_commit_descriptors(epoch)
            .await
            .map_err(|e| {
                DbError::Checkpoint(format!("committer: read descriptors epoch {epoch}: {e}"))
            })?
            .into_iter()
            .filter(|(k, _)| k.ends_with(sink_suffix))
            .map(|(_, v)| v.to_vec())
            .collect();
        if descriptors.is_empty() {
            return Ok(());
        }
        handle
            .commit_aggregated(epoch, descriptors)
            .await
            .map_err(|e| DbError::Checkpoint(format!("committer: commit epoch {epoch}: {e}")))
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
    }

    #[async_trait::async_trait]
    impl SinkConnector for RecordingSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }
        async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
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
            Ok(())
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
            connector: Box::new(RecordingSink { schema, recorded }),
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
            .write_commit_descriptor(epoch, "node=0/sink=ice", 0, Bytes::copy_from_slice(descriptor))
            .await
            .unwrap();
        let key = ["node=0/sink=ice".to_string()];
        assert!(backend.epoch_complete(epoch, &[], &key).await.unwrap());
    }

    #[tokio::test]
    async fn commits_each_sealed_epoch_and_advances_cursor() {
        let backend = Arc::new(InProcessBackend::new(2));
        seal(&backend, 1, b"e1").await;
        seal(&backend, 2, b"e2").await;

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let mut committer = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![("ice".into(), handle)],
        );

        committer.commit_ready().await.unwrap();

        let got = recorded.lock().clone();
        assert_eq!(
            got,
            vec![(1, vec![b"e1".to_vec()]), (2, vec![b"e2".to_vec()])]
        );

        // A second pass with no new sealed epochs is a no-op (cursor advanced).
        committer.commit_ready().await.unwrap();
        assert_eq!(recorded.lock().len(), 2);
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
        );

        committer.commit_ready().await.unwrap();

        // Epoch 2's orphan descriptor must NOT be committed.
        assert_eq!(
            recorded.lock().clone(),
            vec![(1, vec![b"e1".to_vec()]), (3, vec![b"e3".to_vec()])]
        );
    }
}
