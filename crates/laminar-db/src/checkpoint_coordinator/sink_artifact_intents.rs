//! Durable admission and checkpoint packing for pre-begin sink artifact intents.

use futures::{stream::FuturesOrdered, StreamExt};
use laminar_core::checkpoint::{
    checkpoint_artifact_identity_sha256, checkpoint_artifact_intent_sha256, ByteRange,
    CheckpointAttempt, CheckpointSinkArtifactIntent, StateChunkId,
    MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_AGGREGATE_BYTES, MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_BYTES,
    PREPARED_SINK_DESCRIPTOR_VERSION,
};
use laminar_core::checkpoint_decision::CheckpointArtifactInventory;

use super::{
    capture::{PackedArtifact, PackedStateFrames},
    BTreeMap, Bytes, CheckpointCoordinator, DbError, PreparedSinkArtifactIntent,
    PreparedSinkDescriptor,
};

const MAX_SINK_INTENT_CONCURRENCY: usize = 8;

impl CheckpointCoordinator {
    pub(super) async fn persist_sink_artifact_intents_until(
        &mut self,
        inventory: &CheckpointArtifactInventory,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if !inventory.sink_artifact_intent_protocol {
            return Err(DbError::Checkpoint(
                "[LDB-CHECKPOINT-LEGACY-SINK-INTENT] checkpoint-committable epoch predates durable sink artifact admission"
                    .into(),
            ));
        }
        if self.active_sink_artifact_intents.is_some() {
            return Err(DbError::Checkpoint(
                "a prior sink artifact intent remains active".into(),
            ));
        }
        let sink_count = self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable())
            .count();
        if sink_count == 0
            || sink_count > laminar_connectors::connector::MAX_COORDINATED_COMMIT_BATCH_ENTRIES
        {
            return Err(DbError::Checkpoint(format!(
                "checkpoint-committable sink count must be in 1..={}",
                laminar_connectors::connector::MAX_COORDINATED_COMMIT_BATCH_ENTRIES
            )));
        }
        let expected_names = self.committable_sink_names()?;
        let mut pending = self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable());
        let mut active = FuturesOrdered::new();
        for sink in pending.by_ref().take(MAX_SINK_INTENT_CONCURRENCY) {
            active.push_back(capture_sink_intent(sink, inventory.attempt, deadline));
        }
        let mut payloads = BTreeMap::new();
        let mut aggregate_bytes = 0_usize;
        while let Some(result) = active.next().await {
            let (name, payload) = result?;
            let payload_bytes = payload.as_ref().map_or(0, Vec::len);
            if payload_bytes > MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_BYTES {
                return Err(DbError::Checkpoint(format!(
                    "sink '{name}' artifact intent exceeds {MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_BYTES} bytes"
                )));
            }
            aggregate_bytes = aggregate_bytes.checked_add(payload_bytes).ok_or_else(|| {
                DbError::Checkpoint("sink artifact intent byte count overflow".into())
            })?;
            if aggregate_bytes > MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_AGGREGATE_BYTES {
                return Err(DbError::Checkpoint(format!(
                    "sink artifact intents exceed {MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_AGGREGATE_BYTES} aggregate bytes"
                )));
            }
            payloads.insert(name, payload);
            if let Some(sink) = pending.next() {
                active.push_back(capture_sink_intent(sink, inventory.attempt, deadline));
            }
        }
        if !payloads
            .keys()
            .map(String::as_str)
            .eq(expected_names.iter().map(String::as_str))
        {
            return Err(DbError::Checkpoint(
                "sink artifact intents do not match the checkpoint-committable sink roster".into(),
            ));
        }
        let intents = payloads
            .iter()
            .map(|(name, payload)| {
                CheckpointSinkArtifactIntent::try_new(name.clone(), payload.clone())
                    .map_err(DbError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let chunk = StateChunkId {
            participant_id: self.store.participant_id(),
            checkpoint_id: inventory.attempt.checkpoint_id,
        };
        let identity = checkpoint_artifact_identity_sha256(inventory, chunk)?;
        tokio::time::timeout_at(
            deadline,
            self.store
                .save_sink_artifact_intents(chunk, &identity, intents),
        )
        .await
        .map_err(|_| DbError::Checkpoint("sink artifact intent persistence timed out".into()))?
        .map_err(DbError::from)?;
        self.active_sink_artifact_intents = Some(super::ActiveSinkArtifactIntents {
            attempt: inventory.attempt,
            by_sink: payloads,
        });
        Ok(())
    }

    pub(super) fn active_sink_artifact_intents(
        &self,
        attempt: CheckpointAttempt,
        descriptor_names: impl Iterator<Item = String>,
    ) -> Result<&BTreeMap<String, Option<Vec<u8>>>, DbError> {
        let active = self.active_sink_artifact_intents.as_ref().ok_or_else(|| {
            DbError::Checkpoint("checkpoint has no durable sink artifact intents".into())
        })?;
        if active.attempt != attempt || !descriptor_names.eq(active.by_sink.keys().cloned()) {
            return Err(DbError::Checkpoint(
                "sink artifact intents do not match the prepared checkpoint roster".into(),
            ));
        }
        Ok(&active.by_sink)
    }

    pub(super) fn clear_sink_artifact_intents(&mut self, attempt: CheckpointAttempt) {
        if self
            .active_sink_artifact_intents
            .as_ref()
            .is_some_and(|active| active.attempt == attempt)
        {
            self.active_sink_artifact_intents = None;
        }
    }
}

async fn capture_sink_intent(
    sink: &super::RegisteredSink,
    attempt: CheckpointAttempt,
    deadline: tokio::time::Instant,
) -> Result<(String, Option<Vec<u8>>), DbError> {
    let payload = sink
        .handle
        .checkpoint_artifact_intent_until(attempt.epoch, deadline)
        .await
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "sink '{}' artifact intent failed: {error}",
                sink.name
            ))
        })?;
    Ok((sink.name.clone(), payload))
}

pub(super) fn pack_sink_artifacts(
    state: PackedStateFrames,
    intents: &BTreeMap<String, Option<Vec<u8>>>,
    descriptors: &BTreeMap<String, Option<Vec<u8>>>,
) -> Result<PackedArtifact, DbError> {
    let PackedStateFrames {
        mut node_data,
        mut object_length,
        frames,
        current_frame_chunks,
        referenced,
    } = state;
    let mut sink_artifact_intents = Vec::with_capacity(intents.len());
    let mut sink_artifact_intent_chunks = Vec::new();
    for (sink_name, payload) in intents {
        let (range, digest) = pack_payload(
            &mut node_data,
            &mut object_length,
            payload.as_deref(),
            checkpoint_artifact_intent_sha256,
        )?;
        if range.is_some() {
            sink_artifact_intent_chunks.push((sink_artifact_intents.len(), node_data.len() - 1));
        }
        sink_artifact_intents.push(PreparedSinkArtifactIntent {
            sink_name: sink_name.clone(),
            format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
            payload: range,
            sha256: digest,
        });
    }
    let mut prepared_sinks = Vec::with_capacity(descriptors.len());
    let mut prepared_sink_chunks = Vec::new();
    for (sink_name, payload) in descriptors {
        let (range, digest) = pack_payload(
            &mut node_data,
            &mut object_length,
            payload.as_deref(),
            super::checkpoint_descriptor_sha256,
        )?;
        if range.is_some() {
            prepared_sink_chunks.push((prepared_sinks.len(), node_data.len() - 1));
        }
        prepared_sinks.push(PreparedSinkDescriptor {
            sink_name: sink_name.clone(),
            format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
            payload: range,
            sha256: digest,
        });
    }
    Ok(PackedArtifact {
        node_data,
        object_length,
        frames,
        current_frame_chunks,
        referenced,
        sink_artifact_intents,
        sink_artifact_intent_chunks,
        prepared_sinks,
        prepared_sink_chunks,
    })
}

fn pack_payload(
    node_data: &mut Vec<Bytes>,
    object_length: &mut u64,
    payload: Option<&[u8]>,
    digest: fn(Option<&[u8]>) -> String,
) -> Result<(Option<ByteRange>, String), DbError> {
    let Some(payload) = payload else {
        return Ok((None, digest(None)));
    };
    let length = u64::try_from(payload.len())
        .map_err(|_| DbError::Checkpoint("sink artifact payload length exceeds u64".into()))?;
    let range = ByteRange {
        offset: *object_length,
        length,
    };
    *object_length = range
        .end()
        .ok_or_else(|| DbError::Checkpoint("checkpoint node-data length overflow".into()))?;
    node_data.push(Bytes::copy_from_slice(payload));
    Ok((Some(range), digest(Some(payload))))
}
