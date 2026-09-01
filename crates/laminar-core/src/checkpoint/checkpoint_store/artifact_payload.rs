//! Verified range loads for checkpoint-owned sink payloads.

use bytes::Bytes;

use super::{validation::missing_node_data, CheckpointStore, CheckpointStoreError};
use crate::checkpoint::checkpoint_manifest::{ByteRange, CheckpointManifest};

pub(super) async fn load_optional<S: CheckpointStore + ?Sized>(
    store: &S,
    manifest: &CheckpointManifest,
    sink_name: &str,
    range: Option<ByteRange>,
    expected_sha256: &str,
    digest: fn(Option<&[u8]>) -> String,
    label: &str,
) -> Result<Option<Bytes>, CheckpointStoreError> {
    let Some(range) = range else {
        if expected_sha256 != digest(None) {
            return Err(CheckpointStoreError::Invalid(format!(
                "{label} '{sink_name}' absence digest mismatch"
            )));
        }
        return Ok(None);
    };
    let mut payloads = store
        .load_node_data_ranges(
            manifest.node_data.chunk,
            manifest.node_data.object_length,
            &[range],
        )
        .await?
        .ok_or_else(|| missing_node_data(manifest.node_data.chunk))?;
    if payloads.len() != 1 {
        return Err(CheckpointStoreError::Invalid(format!(
            "one {label} range produced {} payloads",
            payloads.len()
        )));
    }
    let Some(bytes) = payloads.pop() else {
        return Err(CheckpointStoreError::Invalid(format!(
            "one {label} range produced no payload"
        )));
    };
    if expected_sha256 != digest(Some(&bytes)) {
        return Err(CheckpointStoreError::Invalid(format!(
            "{label} '{sink_name}' checksum mismatch"
        )));
    }
    Ok(Some(bytes))
}
