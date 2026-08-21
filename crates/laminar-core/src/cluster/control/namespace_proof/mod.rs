use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::ClusterKv;
use crate::checkpoint::{CheckpointParticipant, MAX_CHECKPOINT_PARTICIPANTS};
use crate::cluster::discovery::NodeId;

const NAMESPACE_PROOF_KEY: &str = "control:shared-checkpoint-namespace-proof-v1";
const NAMESPACE_PROOF_VERSION: u8 = 1;
const NAMESPACE_PROOF_MAX_RECORD_BYTES: usize = 512;
const NAMESPACE_PROOF_MAX_SENTINEL_BYTES: u64 = 512;
const NAMESPACE_PROOF_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const NAMESPACE_PROOF_READ_CONCURRENCY: usize = 16;

/// Maximum time allowed for shared checkpoint namespace verification.
pub const MAX_SHARED_NAMESPACE_PROOF_TIMEOUT: Duration = Duration::from_secs(60);

/// Exact checkpoint object-store handle admitted by a successful cluster namespace proof.
#[derive(Clone)]
pub struct VerifiedClusterNamespaces {
    checkpoint: Arc<dyn ObjectStore>,
    local: CheckpointParticipant,
}

impl std::fmt::Debug for VerifiedClusterNamespaces {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedClusterNamespaces")
            .finish_non_exhaustive()
    }
}

impl VerifiedClusterNamespaces {
    /// Return the exact checkpoint store handle covered by the proof.
    #[must_use]
    pub fn checkpoint_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.checkpoint)
    }

    /// Return the process identity that produced this proof.
    #[must_use]
    pub const fn local_participant(&self) -> CheckpointParticipant {
        self.local
    }
}

/// Failure to establish one checkpoint namespace across the startup roster.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct NamespaceProofError {
    message: String,
}

impl NamespaceProofError {
    fn configuration(message: &'static str) -> Self {
        Self {
            message: message.to_string(),
        }
    }

    fn verification(message: &str) -> Self {
        Self {
            message: format!("shared checkpoint namespace proof failed: {message}"),
        }
    }

    fn timeout(timeout: Duration) -> Self {
        Self {
            message: format!("shared checkpoint namespace proof exceeded {timeout:?}"),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NamespaceProofRecord {
    version: u8,
    node_id: u64,
    boot_incarnation: uuid::Uuid,
    nonce: uuid::Uuid,
    roster_sha256: String,
}

impl NamespaceProofRecord {
    fn validate_identity(&self, participant: CheckpointParticipant) -> Result<(), String> {
        if self.version != NAMESPACE_PROOF_VERSION
            || self.node_id != participant.node_id
            || self.boot_incarnation != participant.boot_incarnation
            || self.nonce.is_nil()
            || self.roster_sha256.len() != 64
            || !self
                .roster_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(format!(
                "node {} published a stale or mismatched shared checkpoint namespace proof",
                participant.node_id
            ));
        }
        Ok(())
    }
}

fn namespace_proof_roster_sha256(participants: &[CheckpointParticipant]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut hash = Sha256::new();
    hash.update(b"LAMINAR_SHARED_CHECKPOINT_NAMESPACE_ROSTER_V1\0");
    hash.update(
        u64::try_from(participants.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for participant in participants {
        hash.update(participant.node_id.to_be_bytes());
        hash.update(participant.boot_incarnation.as_bytes());
    }
    let digest = hash.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn namespace_proof_path(node_id: u64) -> object_store::path::Path {
    object_store::path::Path::from(format!(
        "cluster-checkpoint-namespace-proof/v1/node={node_id}/sentinel"
    ))
}

fn namespace_proof_sentinel(record: &NamespaceProofRecord) -> bytes::Bytes {
    bytes::Bytes::from(format!(
        "LAMINAR_SHARED_CHECKPOINT_NAMESPACE_V1\n{}\n{}\n{}\n{}\n",
        record.node_id, record.boot_incarnation, record.nonce, record.roster_sha256
    ))
}

async fn write_namespace_proof_sentinel(
    object_store: &Arc<dyn ObjectStore>,
    record: &NamespaceProofRecord,
) -> Result<(), String> {
    let payload = namespace_proof_sentinel(record);
    if u64::try_from(payload.len()).unwrap_or(u64::MAX) > NAMESPACE_PROOF_MAX_SENTINEL_BYTES {
        return Err("shared checkpoint namespace sentinel exceeds its fixed size bound".into());
    }
    object_store
        .put(
            &namespace_proof_path(record.node_id),
            object_store::PutPayload::from(payload),
        )
        .await
        .map(|_| ())
        .map_err(|error| format!("write checkpoint namespace sentinel: {error}"))
}

async fn read_namespace_proof_sentinel(
    object_store: &Arc<dyn ObjectStore>,
    record: &NamespaceProofRecord,
) -> Result<(), String> {
    let result = object_store
        .get(&namespace_proof_path(record.node_id))
        .await
        .map_err(|error| {
            format!(
                "read node {} checkpoint namespace sentinel: {error}",
                record.node_id
            )
        })?;
    if result.meta.size == 0 || result.meta.size > NAMESPACE_PROOF_MAX_SENTINEL_BYTES {
        return Err(format!(
            "node {} checkpoint namespace sentinel is {} bytes; maximum is {}",
            record.node_id, result.meta.size, NAMESPACE_PROOF_MAX_SENTINEL_BYTES
        ));
    }
    let bytes = result.bytes().await.map_err(|error| {
        format!(
            "read node {} checkpoint namespace sentinel body: {error}",
            record.node_id
        )
    })?;
    if bytes != namespace_proof_sentinel(record) {
        return Err(format!(
            "node {} checkpoint namespace sentinel does not match its boot proof",
            record.node_id
        ));
    }
    Ok(())
}

async fn verify_namespace_proof_visibility(
    control: &Arc<dyn ClusterKv>,
    checkpoint_store: &Arc<dyn ObjectStore>,
    participants: &[CheckpointParticipant],
    local: CheckpointParticipant,
    roster_sha256: &str,
) -> Result<(), String> {
    let checks = futures::stream::iter(participants.iter().copied())
        .map(|participant| async move {
            let encoded = control
                .read_from_checked(NodeId(participant.node_id), NAMESPACE_PROOF_KEY)
                .await?
                .ok_or_else(|| {
                    format!(
                        "node {} has not published its shared checkpoint namespace proof",
                        participant.node_id
                    )
                })?;
            if encoded.len() > NAMESPACE_PROOF_MAX_RECORD_BYTES {
                return Err(format!(
                    "node {} shared checkpoint namespace proof exceeds {} bytes",
                    participant.node_id, NAMESPACE_PROOF_MAX_RECORD_BYTES
                ));
            }
            let record: NamespaceProofRecord = serde_json::from_str(&encoded).map_err(|error| {
                format!(
                    "decode node {} shared checkpoint namespace proof: {error}",
                    participant.node_id
                )
            })?;
            record.validate_identity(participant)?;
            if participant == local && record.roster_sha256 != roster_sha256 {
                return Err(
                    "local shared checkpoint namespace proof has the wrong startup roster".into(),
                );
            }
            read_namespace_proof_sentinel(checkpoint_store, &record).await?;
            Ok::<_, String>(())
        })
        .buffer_unordered(NAMESPACE_PROOF_READ_CONCURRENCY);
    let results: Vec<Result<(), String>> = checks.collect().await;
    for result in results {
        result?;
    }
    Ok(())
}

async fn wait_for_namespace_proof_visibility(
    control: &Arc<dyn ClusterKv>,
    checkpoint_store: &Arc<dyn ObjectStore>,
    participants: &[CheckpointParticipant],
    local: CheckpointParticipant,
    roster_sha256: &str,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    let mut last_failure = "no verification attempt completed".to_string();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "shared checkpoint namespace peer visibility timed out: {last_failure}"
            ));
        }
        match tokio::time::timeout(
            remaining,
            verify_namespace_proof_visibility(
                control,
                checkpoint_store,
                participants,
                local,
                roster_sha256,
            ),
        )
        .await
        {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => last_failure = error,
            Err(_) => {
                return Err(format!(
                    "shared checkpoint namespace peer visibility timed out: {last_failure}"
                ));
            }
        }
        tokio::time::sleep(
            NAMESPACE_PROOF_RETRY_INTERVAL
                .min(deadline.saturating_duration_since(tokio::time::Instant::now())),
        )
        .await;
    }
}

/// Prove that the local process can read every startup participant's retained checkpoint
/// sentinel through its candidate object-store handle.
///
/// # Errors
///
/// Returns an error for an invalid roster, a visibility failure, or a proof timeout.
pub async fn prove_shared_object_store_namespaces(
    local: CheckpointParticipant,
    participants: &[CheckpointParticipant],
    control: Arc<dyn ClusterKv>,
    checkpoint_store: Arc<dyn ObjectStore>,
    timeout: Duration,
) -> Result<VerifiedClusterNamespaces, NamespaceProofError> {
    if participants.is_empty()
        || participants.len() > MAX_CHECKPOINT_PARTICIPANTS
        || participants
            .windows(2)
            .any(|pair| pair[0].node_id >= pair[1].node_id)
        || participants
            .iter()
            .any(|participant| participant.node_id == 0 || participant.boot_incarnation.is_nil())
        || participants
            .iter()
            .filter(|participant| **participant == local)
            .count()
            != 1
    {
        return Err(NamespaceProofError::configuration(
            "shared checkpoint namespace proof requires one canonical exact startup roster",
        ));
    }
    let timeout = timeout.min(MAX_SHARED_NAMESPACE_PROOF_TIMEOUT);
    if timeout.is_zero() {
        return Err(NamespaceProofError::configuration(
            "shared checkpoint namespace proof timeout is zero",
        ));
    }
    let roster_sha256 = namespace_proof_roster_sha256(participants);
    let record = NamespaceProofRecord {
        version: NAMESPACE_PROOF_VERSION,
        node_id: local.node_id,
        boot_incarnation: local.boot_incarnation,
        nonce: uuid::Uuid::new_v4(),
        roster_sha256: roster_sha256.clone(),
    };
    let deadline = tokio::time::Instant::now() + timeout;
    let proof = async {
        write_namespace_proof_sentinel(&checkpoint_store, &record).await?;
        let encoded = serde_json::to_string(&record).map_err(|error| error.to_string())?;
        if encoded.len() > NAMESPACE_PROOF_MAX_RECORD_BYTES {
            return Err(
                "local shared checkpoint namespace proof exceeds its size bound".to_string(),
            );
        }
        control.write_checked(NAMESPACE_PROOF_KEY, encoded).await?;
        wait_for_namespace_proof_visibility(
            &control,
            &checkpoint_store,
            participants,
            local,
            &roster_sha256,
            deadline,
        )
        .await
    };
    match tokio::time::timeout(timeout, proof).await {
        Ok(Ok(())) => Ok(VerifiedClusterNamespaces {
            checkpoint: checkpoint_store,
            local,
        }),
        Ok(Err(error)) => Err(NamespaceProofError::verification(&error)),
        Err(_) => Err(NamespaceProofError::timeout(timeout)),
    }
}

#[cfg(test)]
mod tests;
