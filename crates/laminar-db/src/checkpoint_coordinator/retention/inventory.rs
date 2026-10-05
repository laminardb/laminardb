use super::{
    checkpoint_manifest_bytes, BTreeSet, CheckpointManifest, CheckpointStore,
    CommittedCheckpointIndex, DbError, LiveChunkInventory, StreamExt, TryStreamExt,
    MAX_RETENTION_IO_CONCURRENCY,
};

pub(in crate::checkpoint_coordinator) async fn load_index_manifests(
    store: &dyn CheckpointStore,
    index: &CommittedCheckpointIndex,
) -> Result<Vec<CheckpointManifest>, DbError> {
    let checkpoint_id = index.checkpoint_id;
    let reads = index
        .participants
        .clone()
        .into_iter()
        .map(|participant| async move {
            let manifest = store
                .load_manifest_verified(
                    participant.participant_id,
                    checkpoint_id,
                    participant.manifest_len,
                    &participant.manifest_sha256,
                )
                .await
                .map_err(DbError::from)?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "checkpoint {} participant {} manifest is missing",
                        checkpoint_id, participant.participant_id
                    ))
                })?;
            let encoded = checkpoint_manifest_bytes(&manifest).map_err(|error| {
                DbError::Checkpoint(format!("encode checkpoint manifest: {error}"))
            })?;
            participant
                .verify_manifest(&manifest, &encoded)
                .map_err(DbError::Checkpoint)?;
            Ok::<_, DbError>((participant.participant_id, manifest, encoded))
        });
    let mut loaded = futures::stream::iter(reads)
        .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    loaded.sort_unstable_by_key(|(participant_id, _, _)| *participant_id);
    if let Some((participant_id, _, _)) = loaded.iter().find(|(_, manifest, _)| {
        manifest.epoch != index.epoch
            || manifest.checkpoint_id != index.checkpoint_id
            || manifest.deployment_id != index.deployment_id
            || manifest.pipeline_identity != index.pipeline_identity
            || manifest.vnode_count != index.vnode_count
            || manifest.assignment_fence != index.assignment_fence
    }) {
        return Err(DbError::Checkpoint(format!(
            "checkpoint {} participant {} manifest belongs to a different committed cut",
            index.checkpoint_id, participant_id
        )));
    }
    let views = loaded
        .iter()
        .map(|(_, manifest, bytes)| (manifest, bytes.as_slice()))
        .collect::<Vec<_>>();
    index
        .validate_participant_manifests(&views)
        .map_err(DbError::Checkpoint)?;
    Ok(loaded
        .into_iter()
        .map(|(_, manifest, _)| manifest)
        .collect())
}

pub(in crate::checkpoint_coordinator) fn live_chunk_inventory(
    manifests: &[CheckpointManifest],
) -> LiveChunkInventory {
    let mut references = BTreeSet::new();
    let mut pinned = BTreeSet::new();
    let mut subscription_segments = BTreeSet::new();
    for manifest in manifests {
        pinned.insert(manifest.node_data.chunk);
        for reference in &manifest.referenced_chunks {
            references.insert(reference.chunk);
        }
        if let Some(output) = &manifest.subscription_output {
            for stream in &output.streams {
                subscription_segments.extend(
                    stream
                        .segments
                        .iter()
                        .map(|segment| segment.object_key.clone()),
                );
            }
        }
    }
    LiveChunkInventory {
        references,
        pinned,
        subscription_segments,
    }
}
