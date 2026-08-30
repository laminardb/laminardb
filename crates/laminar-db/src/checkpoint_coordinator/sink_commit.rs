use futures::{stream::FuturesOrdered, StreamExt};
use laminar_connectors::connector::{
    CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
    CoordinatedCommitPayload, MAX_COORDINATED_COMMIT_BATCH_BYTES,
    MAX_COORDINATED_COMMIT_BATCH_ENTRIES, MAX_COORDINATED_COMMIT_PAYLOAD_BYTES,
};
use laminar_core::checkpoint::{
    CheckpointAttempt, CheckpointManifest, CheckpointStore, PipelineIdentity,
    PreparedSinkDescriptor,
};

use super::{CheckpointCoordinator, DbError, RegisteredSink};

const MAX_EXTERNAL_SINK_COMMIT_CONCURRENCY: usize = 8;
pub(super) const MAX_EXTERNAL_SINK_DESCRIPTOR_READ_CONCURRENCY: usize = 8;

#[cfg(all(debug_assertions, feature = "cluster"))]
async fn external_sink_commit_gate(
    sink_name: &str,
    attempt: CheckpointAttempt,
    fencing_token: u64,
) {
    static GATE_FILE: std::sync::OnceLock<Option<std::path::PathBuf>> = std::sync::OnceLock::new();
    let Some(gate_file) = GATE_FILE
        .get_or_init(|| std::env::var_os("LAMINAR_EXTERNAL_SINK_COMMIT_GATE_FILE").map(Into::into))
        .as_ref()
    else {
        return;
    };
    if std::fs::read_to_string(gate_file)
        .ok()
        .is_none_or(|requested| requested.trim() != sink_name)
    {
        return;
    }

    let ready_file = gate_file.with_extension("ready");
    if std::fs::write(
        &ready_file,
        format!(
            "{sink_name} {} {} {fencing_token}",
            attempt.checkpoint_id, attempt.epoch
        ),
    )
    .is_err()
    {
        return;
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    while gate_file.is_file() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let _ = std::fs::remove_file(ready_file);
}

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

        let entries = self
            .load_external_sink_entries(sink, attempt, manifests, deadline)
            .await?;
        let batch = CoordinatedCommitBatch {
            namespace,
            expected_predecessor,
            fencing_token,
            target: attempt,
            entries,
        };
        #[cfg(all(debug_assertions, feature = "cluster"))]
        let has_payload = batch.entries.iter().any(|entry| entry.payload.is_some());
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
            })?;
        #[cfg(all(debug_assertions, feature = "cluster"))]
        if has_payload {
            external_sink_commit_gate(&sink.name, attempt, fencing_token).await;
        }
        Ok(())
    }

    async fn load_external_sink_entries(
        &self,
        sink: &RegisteredSink,
        attempt: CheckpointAttempt,
        manifests: &[&CheckpointManifest],
        deadline: tokio::time::Instant,
    ) -> Result<Vec<CoordinatedCommitPayload>, DbError> {
        let descriptors = validated_external_sink_descriptors(&sink.name, manifests)?;
        let mut pending = descriptors.into_iter();
        let mut active = FuturesOrdered::new();
        for (manifest, descriptor) in pending
            .by_ref()
            .take(MAX_EXTERNAL_SINK_DESCRIPTOR_READ_CONCURRENCY)
        {
            active.push_back(load_external_sink_entry(
                self.store.as_ref(),
                &sink.name,
                attempt,
                manifest,
                descriptor,
                deadline,
            ));
        }
        let mut entries = Vec::with_capacity(manifests.len());
        while let Some(entry) = active.next().await {
            entries.push(entry?);
            if let Some((manifest, descriptor)) = pending.next() {
                active.push_back(load_external_sink_entry(
                    self.store.as_ref(),
                    &sink.name,
                    attempt,
                    manifest,
                    descriptor,
                    deadline,
                ));
            }
        }
        entries.sort_unstable_by_key(|entry| entry.participant_id);
        Ok(entries)
    }
}

async fn load_external_sink_entry(
    store: &dyn CheckpointStore,
    sink_name: &str,
    attempt: CheckpointAttempt,
    manifest: &CheckpointManifest,
    descriptor: &PreparedSinkDescriptor,
    deadline: tokio::time::Instant,
) -> Result<CoordinatedCommitPayload, DbError> {
    let payload = tokio::time::timeout_at(
        deadline,
        store.load_prepared_sink_descriptor(manifest, descriptor),
    )
    .await
    .map_err(|_| {
        DbError::Checkpoint(format!(
            "sink '{sink_name}' descriptor read timed out for participant {}",
            manifest.participant_id
        ))
    })?
    .map_err(DbError::from)?
    .map(|bytes| bytes.to_vec());
    Ok(CoordinatedCommitPayload {
        attempt,
        participant_id: manifest.participant_id,
        payload,
    })
}

pub(super) fn validated_external_sink_descriptors<'a>(
    sink_name: &str,
    manifests: &[&'a CheckpointManifest],
) -> Result<
    Vec<(
        &'a CheckpointManifest,
        &'a laminar_core::checkpoint::PreparedSinkDescriptor,
    )>,
    DbError,
> {
    if manifests.is_empty() || manifests.len() > MAX_COORDINATED_COMMIT_BATCH_ENTRIES {
        return Err(DbError::Checkpoint(format!(
            "sink '{sink_name}' participant count must be in 1..={MAX_COORDINATED_COMMIT_BATCH_ENTRIES}"
        )));
    }
    let per_entry_limit = u64::try_from(MAX_COORDINATED_COMMIT_PAYLOAD_BYTES)
        .map_err(|_| DbError::Checkpoint("external sink payload limit exceeds u64".into()))?;
    let aggregate_limit = u64::try_from(MAX_COORDINATED_COMMIT_BATCH_BYTES)
        .map_err(|_| DbError::Checkpoint("external sink aggregate limit exceeds u64".into()))?;
    let mut aggregate_bytes = 0_u64;
    let mut descriptors = Vec::with_capacity(manifests.len());
    for &manifest in manifests {
        let descriptor = manifest
            .prepared_sinks
            .binary_search_by(|descriptor| descriptor.sink_name.as_str().cmp(sink_name))
            .ok()
            .map(|index| &manifest.prepared_sinks[index])
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "participant {} has no prepared descriptor for sink '{sink_name}'",
                    manifest.participant_id
                ))
            })?;
        if let Some(payload) = descriptor.payload {
            if payload.length > per_entry_limit {
                return Err(DbError::Checkpoint(format!(
                    "sink '{sink_name}' participant {} descriptor exceeds {MAX_COORDINATED_COMMIT_PAYLOAD_BYTES} bytes",
                    manifest.participant_id
                )));
            }
            aggregate_bytes = aggregate_bytes
                .checked_add(payload.length)
                .ok_or_else(|| DbError::Checkpoint("external sink payload byte overflow".into()))?;
            if aggregate_bytes > aggregate_limit {
                return Err(DbError::Checkpoint(format!(
                    "sink '{sink_name}' descriptors exceed {MAX_COORDINATED_COMMIT_BATCH_BYTES} aggregate bytes"
                )));
            }
        }
        descriptors.push((manifest, descriptor));
    }
    Ok(descriptors)
}
