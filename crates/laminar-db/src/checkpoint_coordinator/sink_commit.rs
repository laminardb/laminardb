use futures::{stream::FuturesOrdered, StreamExt};
use laminar_connectors::connector::{
    CoordinatedAbortBatch, CoordinatedAbortDescriptor, CoordinatedAbortEntry,
    CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
    CoordinatedCommitPayload, MAX_COORDINATED_COMMIT_BATCH_BYTES,
    MAX_COORDINATED_COMMIT_BATCH_ENTRIES, MAX_COORDINATED_COMMIT_PAYLOAD_BYTES,
};
use laminar_core::checkpoint::{
    CheckpointAttempt, CheckpointManifest, CheckpointSinkArtifactIntent, CheckpointStore,
    PipelineIdentity, PreparedSinkArtifactIntent, PreparedSinkDescriptor,
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
    pub(super) async fn cleanup_aborted_external_sinks_until(
        &self,
        attempt: CheckpointAttempt,
        participant_ids: &[u64],
        manifests: &[&CheckpointManifest],
        open_intents: &std::collections::BTreeMap<u64, Option<Vec<CheckpointSinkArtifactIntent>>>,
        fencing_token: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if !self.has_checkpoint_committable_sinks() {
            return Ok(());
        }
        if fencing_token == 0 {
            return Err(DbError::Checkpoint(
                "external sink cleanup requires a nonzero fencing token".into(),
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
            active.push_back(self.cleanup_aborted_external_sink_until(
                sink,
                attempt,
                participant_ids,
                manifests,
                open_intents,
                fencing_token,
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
                active.push_back(self.cleanup_aborted_external_sink_until(
                    sink,
                    attempt,
                    participant_ids,
                    manifests,
                    open_intents,
                    fencing_token,
                    &identity,
                    &deployment_id,
                    deadline,
                ));
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    #[allow(clippy::too_many_arguments)] // Exact abort authority stays visible at connector I/O.
    async fn cleanup_aborted_external_sink_until(
        &self,
        sink: &RegisteredSink,
        attempt: CheckpointAttempt,
        participant_ids: &[u64],
        manifests: &[&CheckpointManifest],
        open_intents: &std::collections::BTreeMap<u64, Option<Vec<CheckpointSinkArtifactIntent>>>,
        fencing_token: u64,
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
        let entries = self
            .load_external_sink_abort_entries(
                sink,
                attempt,
                participant_ids,
                manifests,
                open_intents,
                deadline,
            )
            .await?;
        if entries.is_empty() {
            return Ok(());
        }
        let Some(cleaner) = sink.abort_cleaner.as_ref() else {
            if entries.iter().any(|entry| entry.artifact_intent.is_some()) {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-CHECKPOINT-ABORT-CLEANER-MISSING] sink '{}' persisted abort-cleanup evidence but exposed no detached cleaner",
                    sink.name
                )));
            }
            return Ok(());
        };
        if sink
            .abort_cleaner_retired
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(DbError::Checkpoint(format!(
                "sink '{}' abort cleaner was retired after an ambiguous recovery operation",
                sink.name
            )));
        }
        let batch = CoordinatedAbortBatch {
            namespace,
            fencing_token,
            target: attempt,
            entries,
        };
        batch.validate_shape().map_err(|error| {
            DbError::Checkpoint(format!("sink '{}' abort cleanup batch: {error}", sink.name))
        })?;
        let mut cleanup = std::pin::pin!(cleaner.cleanup_aborted(
            batch,
            laminar_connectors::connector::CoordinatedCommitContext::new(deadline),
        ));
        #[cfg(feature = "cluster")]
        let result = if let Some(controller) = self.cluster_controller.as_ref() {
            tokio::select! {
                biased;
                () = controller.wait_for_process_lease_loss() => {
                    sink.abort_cleaner_retired
                        .store(true, std::sync::atomic::Ordering::Release);
                    return Err(DbError::Checkpoint(format!(
                        "sink '{}' lost process authority during aborted artifact cleanup",
                        sink.name
                    )));
                }
                result = tokio::time::timeout_at(deadline, cleanup.as_mut()) => result,
            }
        } else {
            tokio::time::timeout_at(deadline, cleanup.as_mut()).await
        };
        #[cfg(not(feature = "cluster"))]
        let result = tokio::time::timeout_at(deadline, cleanup.as_mut()).await;

        match result {
            Err(_) => {
                sink.abort_cleaner_retired
                    .store(true, std::sync::atomic::Ordering::Release);
                Err(DbError::Checkpoint(format!(
                    "sink '{}' aborted artifact cleanup exceeded its recovery deadline",
                    sink.name
                )))
            }
            Ok(Err(error)) => {
                if error.is_outcome_unknown() {
                    sink.abort_cleaner_retired
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                Err(DbError::Checkpoint(format!(
                    "sink '{}' aborted artifact cleanup failed: {error}",
                    sink.name
                )))
            }
            Ok(Ok(())) => Ok(()),
        }
    }

    pub(super) async fn load_external_sink_abort_entries(
        &self,
        sink: &RegisteredSink,
        attempt: CheckpointAttempt,
        participant_ids: &[u64],
        manifests: &[&CheckpointManifest],
        open_intents: &std::collections::BTreeMap<u64, Option<Vec<CheckpointSinkArtifactIntent>>>,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<CoordinatedAbortEntry>, DbError> {
        if participant_ids.is_empty()
            || participant_ids.len() > MAX_COORDINATED_COMMIT_BATCH_ENTRIES
            || participant_ids.contains(&0)
            || !participant_ids.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(DbError::Checkpoint(
                "aborted sink cleanup participant roster is not canonical".into(),
            ));
        }
        let manifest_count = manifests.len();
        let manifests = manifests
            .iter()
            .map(|manifest| (manifest.participant_id, *manifest))
            .collect::<std::collections::BTreeMap<_, _>>();
        if manifests.len() != manifest_count
            || manifests
                .keys()
                .any(|participant| participant_ids.binary_search(participant).is_err())
            || !open_intents
                .keys()
                .copied()
                .eq(participant_ids.iter().copied())
        {
            return Err(DbError::Checkpoint(
                "aborted sink cleanup evidence does not match the participant roster".into(),
            ));
        }
        validate_abort_payload_bounds(&sink.name, participant_ids, &manifests, open_intents)?;
        let mut pending = participant_ids.iter().copied();
        let mut active = FuturesOrdered::new();
        for participant_id in pending
            .by_ref()
            .take(MAX_EXTERNAL_SINK_DESCRIPTOR_READ_CONCURRENCY)
        {
            active.push_back(load_external_sink_abort_entry(
                self.store.as_ref(),
                &sink.name,
                attempt,
                participant_id,
                manifests.get(&participant_id).copied(),
                open_intents.get(&participant_id),
                deadline,
            ));
        }
        let mut entries = Vec::with_capacity(participant_ids.len());
        while let Some(entry) = active.next().await {
            if let Some(entry) = entry? {
                entries.push(entry);
            }
            if let Some(participant_id) = pending.next() {
                active.push_back(load_external_sink_abort_entry(
                    self.store.as_ref(),
                    &sink.name,
                    attempt,
                    participant_id,
                    manifests.get(&participant_id).copied(),
                    open_intents.get(&participant_id),
                    deadline,
                ));
            }
        }
        Ok(entries)
    }

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

fn validate_abort_payload_bounds(
    sink_name: &str,
    participant_ids: &[u64],
    manifests: &std::collections::BTreeMap<u64, &CheckpointManifest>,
    open_intents: &std::collections::BTreeMap<u64, Option<Vec<CheckpointSinkArtifactIntent>>>,
) -> Result<(), DbError> {
    let per_entry_limit = u64::try_from(MAX_COORDINATED_COMMIT_PAYLOAD_BYTES)
        .map_err(|_| DbError::Checkpoint("external sink payload limit exceeds u64".into()))?;
    let aggregate_limit = u64::try_from(MAX_COORDINATED_COMMIT_BATCH_BYTES)
        .map_err(|_| DbError::Checkpoint("external sink aggregate limit exceeds u64".into()))?;
    let mut aggregate = 0_u64;
    for participant_id in participant_ids {
        let lengths = if let Some(manifest) = manifests.get(participant_id) {
            let descriptor = find_prepared_sink_descriptor(manifest, sink_name)?;
            let intent = find_prepared_sink_artifact_intent(manifest, sink_name);
            [
                descriptor.payload.map(|range| range.length),
                intent.and_then(|intent| intent.payload.map(|range| range.length)),
            ]
        } else {
            let protocol = open_intents.get(participant_id).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aborted participant {participant_id} has no sealed artifact state"
                ))
            })?;
            let intents = protocol.as_ref().ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-CHECKPOINT-LEGACY-SINK-INTENT] aborted participant {participant_id} predates durable sink artifact admission"
                ))
            })?;
            if intents.is_empty() {
                [None, None]
            } else {
                let intent = intents
                    .binary_search_by(|intent| intent.sink_name.as_str().cmp(sink_name))
                    .ok()
                    .map(|index| &intents[index])
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "aborted participant {participant_id} has no artifact intent for sink '{sink_name}'"
                        ))
                    })?;
                let intent_length = intent
                    .payload
                    .as_ref()
                    .map(|payload| {
                        u64::try_from(payload.len()).map_err(|_| {
                            DbError::Checkpoint("external sink payload length exceeds u64".into())
                        })
                    })
                    .transpose()?;
                [None, intent_length]
            }
        };
        for length in lengths.into_iter().flatten() {
            if length > per_entry_limit {
                return Err(DbError::Checkpoint(format!(
                    "sink '{sink_name}' participant {participant_id} abort payload exceeds {MAX_COORDINATED_COMMIT_PAYLOAD_BYTES} bytes"
                )));
            }
            aggregate = aggregate
                .checked_add(length)
                .ok_or_else(|| DbError::Checkpoint("external sink payload byte overflow".into()))?;
            if aggregate > aggregate_limit {
                return Err(DbError::Checkpoint(format!(
                    "sink '{sink_name}' abort payloads exceed {MAX_COORDINATED_COMMIT_BATCH_BYTES} aggregate bytes"
                )));
            }
        }
    }
    Ok(())
}

async fn load_external_sink_abort_entry(
    store: &dyn CheckpointStore,
    sink_name: &str,
    attempt: CheckpointAttempt,
    participant_id: u64,
    manifest: Option<&CheckpointManifest>,
    open_intents: Option<&Option<Vec<CheckpointSinkArtifactIntent>>>,
    deadline: tokio::time::Instant,
) -> Result<Option<CoordinatedAbortEntry>, DbError> {
    let Some(manifest) = manifest else {
        let intent_protocol = open_intents.ok_or_else(|| {
            DbError::Checkpoint(format!(
                "aborted participant {participant_id} has no sealed artifact state"
            ))
        })?;
        let intents = intent_protocol.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-CHECKPOINT-LEGACY-SINK-INTENT] aborted participant {participant_id} predates durable sink artifact admission"
            ))
        })?;
        if intents.is_empty() {
            return Ok(None);
        }
        let intent = intents
            .binary_search_by(|intent| intent.sink_name.as_str().cmp(sink_name))
            .ok()
            .map(|index| &intents[index])
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aborted participant {participant_id} has no artifact intent for sink '{sink_name}'"
                ))
            })?;
        return Ok(Some(CoordinatedAbortEntry {
            attempt,
            participant_id,
            descriptor: CoordinatedAbortDescriptor::Open,
            artifact_intent: intent.payload.clone(),
        }));
    };
    let descriptor = find_prepared_sink_descriptor(manifest, sink_name)?;
    let intent = find_prepared_sink_artifact_intent(manifest, sink_name);
    let descriptor_payload = tokio::time::timeout_at(
        deadline,
        store.load_prepared_sink_descriptor(manifest, descriptor),
    )
    .await
    .map_err(|_| {
        DbError::Checkpoint(format!(
            "sink '{sink_name}' descriptor read timed out for participant {participant_id}"
        ))
    })?
    .map_err(DbError::from)?
    .map(|bytes| bytes.to_vec());
    let artifact_intent = match intent {
        Some(intent) => {
            tokio::time::timeout_at(deadline, store.load_sink_artifact_intent(manifest, intent))
                .await
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                "sink '{sink_name}' artifact intent read timed out for participant {participant_id}"
            ))
                })?
                .map_err(DbError::from)?
                .map(|bytes| bytes.to_vec())
        }
        None => None,
    };
    Ok(Some(CoordinatedAbortEntry {
        attempt,
        participant_id,
        descriptor: CoordinatedAbortDescriptor::Prepared(descriptor_payload),
        artifact_intent,
    }))
}

fn find_prepared_sink_descriptor<'a>(
    manifest: &'a CheckpointManifest,
    sink_name: &str,
) -> Result<&'a PreparedSinkDescriptor, DbError> {
    manifest
        .prepared_sinks
        .binary_search_by(|descriptor| descriptor.sink_name.as_str().cmp(sink_name))
        .ok()
        .map(|index| &manifest.prepared_sinks[index])
        .ok_or_else(|| {
            DbError::Checkpoint(format!(
                "participant {} has no prepared descriptor for sink '{sink_name}'",
                manifest.participant_id
            ))
        })
}

fn find_prepared_sink_artifact_intent<'a>(
    manifest: &'a CheckpointManifest,
    sink_name: &str,
) -> Option<&'a PreparedSinkArtifactIntent> {
    manifest
        .sink_artifact_intents
        .binary_search_by(|intent| intent.sink_name.as_str().cmp(sink_name))
        .ok()
        .map(|index| &manifest.sink_artifact_intents[index])
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
