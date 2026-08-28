use futures::{stream::FuturesOrdered, StreamExt};
use laminar_connectors::connector::{
    CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
    CoordinatedCommitPayload,
};
use laminar_core::checkpoint::{CheckpointAttempt, CheckpointManifest, PipelineIdentity};

use super::{CheckpointCoordinator, DbError, RegisteredSink};

const MAX_EXTERNAL_SINK_COMMIT_CONCURRENCY: usize = 8;

impl CheckpointCoordinator {
    pub(super) async fn commit_external_sinks_until(
        &self,
        attempt: CheckpointAttempt,
        manifests: &[&CheckpointManifest],
        fencing_token: u64,
        predecessor_checkpoint_id: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if !self.has_checkpoint_committable_sinks() {
            return Ok(());
        }
        if fencing_token == 0 {
            return Err(DbError::Checkpoint(
                "external checkpoint publication requires a nonzero fencing token".into(),
            ));
        }
        let identity = self.expected_pipeline_identity()?;
        let deployment_id = self.expected_deployment_id()?.to_owned();
        let mut pending = self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable());
        let mut active = FuturesOrdered::new();
        for sink in pending.by_ref().take(MAX_EXTERNAL_SINK_COMMIT_CONCURRENCY) {
            active.push_back(self.commit_external_sink_until(
                sink,
                attempt,
                manifests,
                fencing_token,
                predecessor_checkpoint_id,
                &identity,
                &deployment_id,
                deadline,
            ));
        }
        let mut first_error = None;
        while let Some(result) = active.next().await {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
            if let Some(sink) = pending.next() {
                active.push_back(self.commit_external_sink_until(
                    sink,
                    attempt,
                    manifests,
                    fencing_token,
                    predecessor_checkpoint_id,
                    &identity,
                    &deployment_id,
                    deadline,
                ));
            }
        }

        // RECOVERY: durable Commit cannot be rolled back. Attempt and drain every sink even after
        // one failure; ordered completion preserves the registration-order first error.
        first_error.map_or(Ok(()), Err)
    }

    #[allow(clippy::too_many_arguments)] // Protocol authority stays explicit at the I/O boundary.
    async fn commit_external_sink_until(
        &self,
        sink: &RegisteredSink,
        attempt: CheckpointAttempt,
        manifests: &[&CheckpointManifest],
        fencing_token: u64,
        predecessor_checkpoint_id: u64,
        identity: &PipelineIdentity,
        deployment_id: &str,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let namespace = CoordinatedCommitNamespace::try_new(
            identity.clone(),
            deployment_id.to_owned(),
            sink.name.clone(),
        )
        .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        let cursor =
            tokio::time::timeout_at(deadline, sink.handle.committed_cursor(namespace.clone()))
                .await
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "sink '{}' committed-cursor read timed out",
                        sink.name
                    ))
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "sink '{}' committed-cursor read failed: {error}",
                        sink.name
                    ))
                })?;
        if let Some(cursor) = cursor {
            if cursor.checkpoint_id == attempt.checkpoint_id {
                if cursor.fencing_token != fencing_token {
                    return Err(DbError::Checkpoint(format!(
                        "sink '{}' checkpoint {} was committed under fencing token {}, expected {fencing_token}",
                        sink.name, attempt.checkpoint_id, cursor.fencing_token
                    )));
                }
                return Ok(());
            }
        }
        let expected_predecessor = if predecessor_checkpoint_id == 0 {
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            }
        } else {
            let cursor = cursor.ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "sink '{}' has no external cursor for committed predecessor {predecessor_checkpoint_id}",
                    sink.name
                ))
            })?;
            if cursor.checkpoint_id != predecessor_checkpoint_id {
                return Err(DbError::Checkpoint(format!(
                    "sink '{}' external cursor {} trails committed predecessor {predecessor_checkpoint_id}; recovery must publish the missing cut first",
                    sink.name, cursor.checkpoint_id
                )));
            }
            cursor
        };

        let mut entries = Vec::with_capacity(manifests.len());
        for manifest in manifests {
            let descriptor = manifest
                .prepared_sinks
                .binary_search_by(|descriptor| descriptor.sink_name.cmp(&sink.name))
                .ok()
                .map(|index| &manifest.prepared_sinks[index])
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "participant {} has no prepared descriptor for sink '{}'",
                        manifest.participant_id, sink.name
                    ))
                })?;
            let payload = tokio::time::timeout_at(
                deadline,
                self.store
                    .load_prepared_sink_descriptor(manifest, descriptor),
            )
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "sink '{}' descriptor read timed out for participant {}",
                    sink.name, manifest.participant_id
                ))
            })?
            .map_err(DbError::from)?
            .map(|bytes| bytes.to_vec());
            entries.push(CoordinatedCommitPayload {
                attempt,
                participant_id: manifest.participant_id,
                payload,
            });
        }
        entries.sort_unstable_by_key(|entry| entry.participant_id);
        let batch = CoordinatedCommitBatch {
            namespace,
            expected_predecessor,
            fencing_token,
            target: attempt,
            entries,
        };
        batch.validate_shape().map_err(|error| {
            DbError::Checkpoint(format!("sink '{}' commit batch: {error}", sink.name))
        })?;
        tokio::time::timeout_at(deadline, sink.handle.commit_aggregated(batch))
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!("sink '{}' external commit timed out", sink.name))
            })?
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "sink '{}' external commit failed: {error}",
                    sink.name
                ))
            })
    }
}
