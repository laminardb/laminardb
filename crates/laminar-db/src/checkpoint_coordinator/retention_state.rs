//! Bounded validation of the exact state objects protecting an artifact floor.

use std::collections::BTreeMap;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use laminar_core::checkpoint::{ByteRange, CheckpointManifest, CheckpointStore, StateChunkId};
use sha2::{Digest, Sha256};

use super::MAX_RETENTION_IO_CONCURRENCY;
use crate::error::DbError;

const READ_BYTES: u64 = 256 * 1024;
const MAX_CHUNKS: usize = 8192;
const MAX_TOTAL_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) async fn validate_retained_state(
    store: &dyn CheckpointStore,
    manifests: &[CheckpointManifest],
) -> Result<(), DbError> {
    let mut chunks = BTreeMap::new();
    for manifest in manifests {
        let objects = std::iter::once((
            manifest.node_data.chunk,
            manifest.node_data.object_length,
            &manifest.node_data.sha256,
        ))
        .chain(
            manifest
                .referenced_chunks
                .iter()
                .map(|reference| (reference.chunk, reference.object_length, &reference.sha256)),
        );
        for (chunk, length, digest) in objects {
            if let Some(previous) = chunks.insert(chunk, (length, digest.clone())) {
                if previous != (length, digest.clone()) {
                    return Err(DbError::Checkpoint(format!(
                        "retained state chunk {chunk:?} has divergent length or digest"
                    )));
                }
            }
            if chunks.len() > MAX_CHUNKS {
                return Err(DbError::Checkpoint(
                    "retained state preflight exceeds its 8192-object bound; artifacts retained"
                        .into(),
                ));
            }
        }
    }
    let total = chunks
        .values()
        .try_fold(0_u64, |total, (length, _)| total.checked_add(*length));
    if total.is_none_or(|total| total > MAX_TOTAL_BYTES) {
        return Err(DbError::Checkpoint(
            "retained state preflight exceeds its 4 GiB read bound; artifacts retained".into(),
        ));
    }
    tokio::time::timeout(PREFLIGHT_TIMEOUT, async {
        futures::stream::iter(chunks)
            .map(|(chunk, (length, digest))| verify_chunk(store, chunk, length, digest))
            .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await
            .map(|_| ())
    })
    .await
    .map_err(|_| {
        DbError::Checkpoint("retained state preflight timed out; artifacts retained".into())
    })?
}

async fn verify_chunk(
    store: &dyn CheckpointStore,
    chunk: StateChunkId,
    length: u64,
    expected_digest: String,
) -> Result<(), DbError> {
    let mut digest = Sha256::new();
    let mut offset = 0;
    // An empty object still has to exist; a missing empty payload is not a valid checkpoint.
    if length == 0 && store.load_node_data_ranges(chunk, 0, &[]).await?.is_none() {
        return Err(DbError::Checkpoint(format!(
            "retained empty state chunk {chunk:?} is missing"
        )));
    }
    while offset < length {
        let count = READ_BYTES.min(length - offset);
        let ranges = [ByteRange {
            offset,
            length: count,
        }];
        let payload = store
            .load_node_data_ranges(chunk, length, &ranges)
            .await?
            .ok_or_else(|| {
                DbError::Checkpoint(format!("retained state chunk {chunk:?} is missing"))
            })?;
        if payload.len() != 1 || u64::try_from(payload[0].len()).ok() != Some(count) {
            return Err(DbError::Checkpoint(format!(
                "retained state chunk {chunk:?} returned an incomplete range"
            )));
        }
        digest.update(&payload[0]);
        offset += count;
    }
    if format!("{:x}", digest.finalize()) != expected_digest {
        return Err(DbError::Checkpoint(format!(
            "retained state chunk {chunk:?} failed its complete-object digest"
        )));
    }
    Ok(())
}
