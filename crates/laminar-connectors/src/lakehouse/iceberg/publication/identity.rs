use std::collections::HashMap;

use iceberg::spec::DataFile;
use sha2::{Digest, Sha256};

use crate::connector::{CoordinatedCommitBatch, CoordinatedCommitCursor};
use crate::error::ConnectorError;

use super::super::descriptor::{data_file_fingerprint, IcebergFileFingerprintV1};

pub(super) const SUMMARY_NAMESPACE: &str = "laminardb.commit.namespace";
pub(super) const SUMMARY_CHECKPOINT: &str = "laminardb.checkpoint.id";
pub(super) const SUMMARY_FENCE: &str = "laminardb.fencing.token";
pub(super) const SUMMARY_BATCH_FINGERPRINT: &str = "laminardb.batch.fingerprint";
pub(super) const SUMMARY_FILE_SET: &str = "laminardb.file-set.fingerprint";
const SUMMARY_DEPLOYMENT: &str = "laminardb.deployment.id";
const SUMMARY_SINK: &str = "laminardb.sink.id";
pub(super) const SUMMARY_COMMIT_UUID: &str = "laminardb.commit.uuid";

pub(super) struct PublicationIdentity {
    pub(super) exact_batch_hex: String,
    pub(super) external_key: String,
    pub(super) commit_uuid: uuid::Uuid,
    pub(super) target: CoordinatedCommitCursor,
}

pub(super) fn summary_properties(
    batch: &CoordinatedCommitBatch,
    external_key: &str,
    exact_batch_fingerprint: &str,
    file_set_fingerprint: &str,
    commit_uuid: uuid::Uuid,
) -> HashMap<String, String> {
    HashMap::from([
        (SUMMARY_NAMESPACE.into(), external_key.into()),
        (
            SUMMARY_CHECKPOINT.into(),
            batch.target.checkpoint_id.to_string(),
        ),
        (SUMMARY_FENCE.into(), batch.fencing_token.to_string()),
        (
            SUMMARY_BATCH_FINGERPRINT.into(),
            exact_batch_fingerprint.into(),
        ),
        (SUMMARY_FILE_SET.into(), file_set_fingerprint.into()),
        (
            SUMMARY_DEPLOYMENT.into(),
            batch.namespace.deployment_id.clone(),
        ),
        (SUMMARY_SINK.into(), batch.namespace.sink_id.clone()),
        (SUMMARY_COMMIT_UUID.into(), commit_uuid.to_string()),
    ])
}

pub(super) fn data_file_set_fingerprint(files: &[DataFile]) -> String {
    let fingerprints = files.iter().map(data_file_fingerprint).collect::<Vec<_>>();
    file_set_fingerprint(&fingerprints)
}

pub(super) fn file_set_fingerprint(files: &[IcebergFileFingerprintV1]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"laminardb-iceberg-file-set-v1\0");
    let mut files = files.iter().collect::<Vec<_>>();
    files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    for file in files {
        hash.update(file.path.len().to_be_bytes());
        hash.update(file.path.as_bytes());
        hash.update(file.metadata_sha256.len().to_be_bytes());
        hash.update(file.metadata_sha256.as_bytes());
        hash.update(file.records.to_be_bytes());
        hash.update(file.bytes.to_be_bytes());
    }
    format!("{:x}", hash.finalize())
}

pub(super) fn deterministic_commit_uuid(
    batch: &CoordinatedCommitBatch,
    exact_batch_fingerprint: &[u8; 32],
) -> uuid::Uuid {
    let mut hash = Sha256::new();
    hash.update(b"laminardb-iceberg-commit-uuid-v1\0");
    hash.update(batch.namespace.external_key().as_bytes());
    hash.update(batch.target.checkpoint_id.to_be_bytes());
    hash.update(batch.fencing_token.to_be_bytes());
    hash.update(exact_batch_fingerprint);
    let digest = hash.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

pub(super) fn deterministic_idempotency_key(
    batch: &CoordinatedCommitBatch,
    logical_commit_uuid: uuid::Uuid,
    attempt: usize,
) -> Result<uuid::Uuid, ConnectorError> {
    let deployment = uuid::Uuid::parse_str(&batch.namespace.deployment_id).map_err(|_| {
        ConnectorError::TransactionError(
            "Iceberg coordinated publication has an invalid deployment UUID".into(),
        )
    })?;
    if deployment.get_version_num() != 7 {
        return Err(ConnectorError::TransactionError(
            "Iceberg REST idempotency requires a UUIDv7 deployment identity".into(),
        ));
    }
    let mut hash = Sha256::new();
    hash.update(b"laminardb-iceberg-rest-idempotency-v1\0");
    hash.update(logical_commit_uuid.as_bytes());
    hash.update(attempt.to_be_bytes());
    let digest = hash.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[..6].copy_from_slice(&deployment.as_bytes()[..6]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(uuid::Uuid::from_bytes(bytes))
}

pub(super) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(value, "{byte:02x}");
    }
    value
}
