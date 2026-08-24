//! Immutable subscription segment persistence and validation.

use bytes::Bytes;
use futures::StreamExt;
use object_store::{ObjectStoreExt, PutPayload};

use super::{CheckpointStoreError, ObjectStoreCheckpointStore};
use crate::checkpoint::{
    OutputSegmentRef, SubscriptionDigest, MAX_OUTPUT_FRAMES_PER_SEGMENT, MAX_OUTPUT_SEGMENT_BYTES,
};

const MAX_ORPHAN_SCAN_OBJECTS: u64 = 1_000_000;

/// Result of one bounded subscription-segment orphan scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SubscriptionOrphanCleanup {
    /// Objects examined under the subscription-output namespace.
    pub objects_scanned: u64,
    /// Grace-expired, unreachable objects deleted by exact key.
    pub objects_deleted: u64,
    /// Encoded bytes represented by deleted object metadata.
    pub bytes_deleted: u64,
    /// Unreachable bytes retained because their orphan grace period has not elapsed.
    pub bytes_remaining: u64,
}

pub(super) async fn save(
    store: &ObjectStoreCheckpointStore,
    segment: &OutputSegmentRef,
    payload: Bytes,
) -> Result<(), CheckpointStoreError> {
    validate_reference(segment)?;
    validate_payload(segment, &payload)?;
    let path = segment_path(store, &segment.object_key)?;
    if store
        .create_immutable(&path, PutPayload::from_bytes(payload.clone()))
        .await?
    {
        return Ok(());
    }
    match load(store, segment).await? {
        Some(existing) if existing == payload => Ok(()),
        Some(_) => Err(CheckpointStoreError::Invalid(format!(
            "subscription segment '{}' already exists with conflicting immutable content",
            segment.object_key
        ))),
        None => Err(CheckpointStoreError::Invalid(format!(
            "subscription segment '{}' create conflicted but no object exists",
            segment.object_key
        ))),
    }
}

pub(super) async fn load(
    store: &ObjectStoreCheckpointStore,
    segment: &OutputSegmentRef,
) -> Result<Option<Bytes>, CheckpointStoreError> {
    validate_reference(segment)?;
    let path = segment_path(store, &segment.object_key)?;
    let result = match store.store.get(&path).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if result.meta.size != segment.encoded_length {
        return Err(CheckpointStoreError::Invalid(format!(
            "subscription segment '{}' is {} bytes; expected {}",
            segment.object_key, result.meta.size, segment.encoded_length
        )));
    }
    let bytes = result.bytes().await?;
    validate_payload(segment, &bytes)?;
    Ok(Some(bytes))
}

pub(super) async fn delete(
    store: &ObjectStoreCheckpointStore,
    object_key: &str,
) -> Result<(), CheckpointStoreError> {
    let path = segment_path(store, object_key)?;
    store.delete_exact(&path).await
}

pub(super) async fn delete_orphans(
    store: &ObjectStoreCheckpointStore,
    reachable: &std::collections::BTreeSet<String>,
    through_checkpoint_id: u64,
    grace_before_ms: i64,
) -> Result<SubscriptionOrphanCleanup, CheckpointStoreError> {
    if through_checkpoint_id == 0 || grace_before_ms <= 0 {
        return Err(CheckpointStoreError::Invalid(
            "subscription orphan cleanup authority is not canonical".into(),
        ));
    }
    let prefix = object_store::path::Path::from(format!("{}subscription-output", store.prefix));
    let mut listed = store.store.list(Some(&prefix));
    let mut report = SubscriptionOrphanCleanup::default();
    while let Some(entry) = listed.next().await {
        let entry = entry?;
        report.objects_scanned = report.objects_scanned.checked_add(1).ok_or_else(|| {
            CheckpointStoreError::Invalid("subscription orphan scan count overflow".into())
        })?;
        if report.objects_scanned > MAX_ORPHAN_SCAN_OBJECTS {
            return Err(CheckpointStoreError::Invalid(format!(
                "subscription orphan scan exceeds {MAX_ORPHAN_SCAN_OBJECTS} objects"
            )));
        }
        let full_key = entry.location.to_string();
        let object_key = full_key.strip_prefix(&store.prefix).ok_or_else(|| {
            CheckpointStoreError::Invalid(
                "subscription object lies outside its checkpoint namespace".into(),
            )
        })?;
        let checkpoint_id = checkpoint_id_from_canonical_key(object_key)?;
        if checkpoint_id > through_checkpoint_id || reachable.contains(object_key) {
            continue;
        }
        if entry.last_modified.timestamp_millis() > grace_before_ms {
            report.bytes_remaining =
                report
                    .bytes_remaining
                    .checked_add(entry.size)
                    .ok_or_else(|| {
                        CheckpointStoreError::Invalid(
                            "subscription orphan byte count overflow".into(),
                        )
                    })?;
            continue;
        }
        store.delete_exact(&entry.location).await?;
        report.objects_deleted = report.objects_deleted.checked_add(1).ok_or_else(|| {
            CheckpointStoreError::Invalid("subscription orphan delete count overflow".into())
        })?;
        report.bytes_deleted = report
            .bytes_deleted
            .checked_add(entry.size)
            .ok_or_else(|| {
                CheckpointStoreError::Invalid("subscription orphan byte count overflow".into())
            })?;
    }
    Ok(report)
}

fn segment_path(
    store: &ObjectStoreCheckpointStore,
    object_key: &str,
) -> Result<object_store::path::Path, CheckpointStoreError> {
    validate_object_key(object_key)?;
    Ok(object_store::path::Path::from(format!(
        "{}{object_key}",
        store.prefix
    )))
}

fn validate_object_key(object_key: &str) -> Result<(), CheckpointStoreError> {
    if object_key.is_empty()
        || object_key.len() > 2_048
        || !object_key.starts_with("subscription-output/")
        || object_key.starts_with('/')
        || object_key.contains('\\')
        || object_key
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(CheckpointStoreError::Invalid(
            "subscription segment object key is not canonical".into(),
        ));
    }
    Ok(())
}

fn checkpoint_id_from_canonical_key(object_key: &str) -> Result<u64, CheckpointStoreError> {
    let components = object_key.split('/').collect::<Vec<_>>();
    let canonical = components.len() == 7
        && components[0] == "subscription-output"
        && uuid::Uuid::parse_str(components[1])
            .is_ok_and(|deployment| deployment.to_string() == components[1])
        && is_lower_sha256(components[2])
        && is_lower_sha256(components[3])
        && components[4]
            .parse::<u16>()
            .is_ok_and(|partition| partition.to_string() == components[4]);
    let checkpoint_id = components
        .get(5)
        .and_then(|component| component.strip_prefix("checkpoint="))
        .filter(|value| value.len() == 20)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|checkpoint_id| *checkpoint_id != 0);
    if !canonical
        || checkpoint_id.is_none()
        || !components
            .get(6)
            .is_some_and(|filename| canonical_segment_filename(filename))
    {
        return Err(CheckpointStoreError::Invalid(
            "listed subscription segment object key is not canonical".into(),
        ));
    }
    checkpoint_id.ok_or_else(|| {
        CheckpointStoreError::Invalid("subscription segment checkpoint ID is absent".into())
    })
}

fn canonical_segment_filename(filename: &str) -> bool {
    let Some(stem) = filename.strip_suffix(".arrow") else {
        return false;
    };
    let parts = stem.split('-').collect::<Vec<_>>();
    if parts.len() != 3
        || parts[0].len() != 20
        || parts[1].len() != 20
        || !is_lower_sha256(parts[2])
    {
        return false;
    }
    let Some(first) = parts[0].parse::<u64>().ok() else {
        return false;
    };
    parts[1].parse::<u64>().is_ok_and(|end| first < end)
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_reference(segment: &OutputSegmentRef) -> Result<(), CheckpointStoreError> {
    validate_object_key(&segment.object_key)?;
    if segment.encoded_length == 0
        || segment.encoded_length > u64::try_from(MAX_OUTPUT_SEGMENT_BYTES).unwrap_or(u64::MAX)
        || segment.frame_count == 0
        || segment.frame_count > MAX_OUTPUT_FRAMES_PER_SEGMENT
        || segment.row_count == 0
        || segment.first_sequence >= segment.exclusive_end_sequence
        || segment
            .exclusive_end_sequence
            .get()
            .checked_sub(segment.first_sequence.get())
            != Some(segment.frame_count)
    {
        return Err(CheckpointStoreError::Invalid(
            "subscription segment reference is not canonical".into(),
        ));
    }
    Ok(())
}

fn validate_payload(
    segment: &OutputSegmentRef,
    payload: &[u8],
) -> Result<(), CheckpointStoreError> {
    if u64::try_from(payload.len()).ok() != Some(segment.encoded_length) {
        return Err(CheckpointStoreError::Invalid(format!(
            "subscription segment '{}' payload length mismatch",
            segment.object_key
        )));
    }
    let digest = SubscriptionDigest::for_bytes(b"laminardb-subscription-segment-v1", payload);
    if digest != segment.payload_digest {
        return Err(CheckpointStoreError::Invalid(format!(
            "subscription segment '{}' payload digest mismatch",
            segment.object_key
        )));
    }
    Ok(())
}
