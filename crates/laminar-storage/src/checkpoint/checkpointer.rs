//! Async checkpoint persistence via object stores (F-DCKP-004).
//!
//! The [`Checkpointer`] trait abstracts checkpoint I/O so that the
//! checkpoint coordinator doesn't need to know whether state is persisted
//! to local disk, S3, GCS, or Azure Blob.
//!
//! [`ObjectStoreCheckpointer`] is the production implementation that
//! writes to any `object_store::ObjectStore` backend with:
//! - Concurrent partition uploads via `JoinSet`
//! - SHA-256 integrity digests
//! - Exponential backoff retry on transient errors

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, PutOptions, PutPayload};
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;

use super::layout::{
    CheckpointId, CheckpointManifestV2, CheckpointPaths, PartitionSnapshotEntry,
};

/// Errors from checkpoint persistence operations.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointerError {
    /// Object store I/O error.
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// JSON serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// SHA-256 integrity check failed.
    #[error("integrity check failed for {path}: expected {expected}, got {actual}")]
    IntegrityMismatch {
        /// Object path.
        path: String,
        /// Expected SHA-256 hex digest.
        expected: String,
        /// Actual SHA-256 hex digest.
        actual: String,
    },

    /// A concurrent upload task failed.
    #[error("upload task failed: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

/// Async trait for checkpoint persistence operations.
///
/// Implementations handle writing/reading checkpoint artifacts to
/// durable storage. The checkpoint coordinator calls these methods
/// during the checkpoint commit protocol.
#[async_trait]
pub trait Checkpointer: Send + Sync {
    /// Write a manifest to the checkpoint store.
    async fn save_manifest(
        &self,
        manifest: &CheckpointManifestV2,
    ) -> Result<(), CheckpointerError>;

    /// Load a manifest by checkpoint ID.
    async fn load_manifest(
        &self,
        id: &CheckpointId,
    ) -> Result<CheckpointManifestV2, CheckpointerError>;

    /// Write a state snapshot for a single operator partition.
    ///
    /// Returns the SHA-256 hex digest of the written data.
    async fn save_snapshot(
        &self,
        id: &CheckpointId,
        operator: &str,
        partition: u32,
        data: Bytes,
    ) -> Result<String, CheckpointerError>;

    /// Write an incremental delta for a single operator partition.
    ///
    /// Returns the SHA-256 hex digest of the written data.
    async fn save_delta(
        &self,
        id: &CheckpointId,
        operator: &str,
        partition: u32,
        data: Bytes,
    ) -> Result<String, CheckpointerError>;

    /// Load a snapshot or delta by path.
    async fn load_artifact(&self, path: &str) -> Result<Bytes, CheckpointerError>;

    /// Update the `_latest` pointer to the given checkpoint.
    async fn update_latest(&self, id: &CheckpointId) -> Result<(), CheckpointerError>;

    /// Read the `_latest` pointer to find the most recent checkpoint.
    async fn read_latest(&self) -> Result<Option<CheckpointId>, CheckpointerError>;

    /// List all checkpoint IDs (sorted chronologically, oldest first).
    async fn list_checkpoints(&self) -> Result<Vec<CheckpointId>, CheckpointerError>;

    /// Delete a checkpoint and all its artifacts.
    async fn delete_checkpoint(&self, id: &CheckpointId) -> Result<(), CheckpointerError>;
}

/// Production [`Checkpointer`] backed by an [`ObjectStore`].
///
/// Supports concurrent partition uploads and SHA-256 integrity verification.
pub struct ObjectStoreCheckpointer {
    /// The underlying object store.
    store: Arc<dyn ObjectStore>,
    /// Path generator for checkpoint artifacts.
    paths: CheckpointPaths,
    /// Maximum number of concurrent uploads.
    max_concurrent_uploads: usize,
}

impl ObjectStoreCheckpointer {
    /// Create a new checkpointer.
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        paths: CheckpointPaths,
        max_concurrent_uploads: usize,
    ) -> Self {
        Self {
            store,
            paths,
            max_concurrent_uploads,
        }
    }

    /// Write data to a path with exponential backoff retry.
    async fn put_with_retry(
        store: &dyn ObjectStore,
        path: &Path,
        data: PutPayload,
    ) -> Result<(), CheckpointerError> {
        let op = || async {
            store
                .put_opts(path, data.clone(), PutOptions::default())
                .await
                .map_err(|e| match &e {
                    object_store::Error::Generic { .. } => {
                        backoff::Error::transient(CheckpointerError::ObjectStore(e))
                    }
                    _ => backoff::Error::permanent(CheckpointerError::ObjectStore(e)),
                })?;
            Ok(())
        };

        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(std::time::Duration::from_secs(30)))
            .build();

        backoff::future::retry(backoff, op).await
    }

    /// Compute SHA-256 hex digest of data.
    fn sha256_hex(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }

    /// Write data to a path and return its SHA-256 digest.
    async fn write_with_digest(
        &self,
        path_str: &str,
        data: Bytes,
    ) -> Result<String, CheckpointerError> {
        let digest = Self::sha256_hex(&data);
        let path = Path::from(path_str);
        let payload = PutPayload::from_bytes(data);
        Self::put_with_retry(self.store.as_ref(), &path, payload).await?;
        Ok(digest)
    }

    /// Save multiple operator partition snapshots concurrently.
    ///
    /// Returns a map of `(operator, partition) -> PartitionSnapshotEntry`.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointerError`] if any upload or join fails.
    pub async fn save_partitions_concurrent(
        &self,
        id: &CheckpointId,
        snapshots: Vec<(String, u32, bool, Bytes)>,
    ) -> Result<Vec<(String, PartitionSnapshotEntry)>, CheckpointerError> {
        let mut join_set = JoinSet::new();
        let store = Arc::clone(&self.store);

        for (operator, partition, is_delta, data) in snapshots {
            let path_str = if is_delta {
                self.paths.delta(id, &operator, partition)
            } else {
                self.paths.snapshot(id, &operator, partition)
            };

            let store = Arc::clone(&store);
            let data_len = data.len() as u64;

            // Limit concurrency by awaiting when at max
            if join_set.len() >= self.max_concurrent_uploads {
                if let Some(result) = join_set.join_next().await {
                    result??;
                }
            }

            join_set.spawn(async move {
                let digest = {
                    let mut hasher = Sha256::new();
                    hasher.update(&data);
                    format!("{:x}", hasher.finalize())
                };
                let path = Path::from(path_str.as_str());
                let payload = PutPayload::from_bytes(data);
                Self::put_with_retry(store.as_ref(), &path, payload).await?;

                Ok::<_, CheckpointerError>((
                    operator,
                    PartitionSnapshotEntry {
                        partition_id: partition,
                        is_delta,
                        path: path_str,
                        size_bytes: data_len,
                        sha256: Some(digest),
                    },
                ))
            });
        }

        // Collect remaining results
        let mut entries = Vec::new();
        while let Some(result) = join_set.join_next().await {
            entries.push(result??);
        }

        Ok(entries)
    }
}

#[async_trait]
impl Checkpointer for ObjectStoreCheckpointer {
    async fn save_manifest(
        &self,
        manifest: &CheckpointManifestV2,
    ) -> Result<(), CheckpointerError> {
        let json = serde_json::to_vec_pretty(manifest)?;
        let path_str = self.paths.manifest(&manifest.checkpoint_id);
        let path = Path::from(path_str.as_str());
        let payload = PutPayload::from_bytes(Bytes::from(json));
        Self::put_with_retry(self.store.as_ref(), &path, payload).await
    }

    async fn load_manifest(
        &self,
        id: &CheckpointId,
    ) -> Result<CheckpointManifestV2, CheckpointerError> {
        let path_str = self.paths.manifest(id);
        let path = Path::from(path_str.as_str());
        let result = self.store.get_opts(&path, GetOptions::default()).await?;
        let data = result.bytes().await?;
        let manifest: CheckpointManifestV2 = serde_json::from_slice(&data)?;
        Ok(manifest)
    }

    async fn save_snapshot(
        &self,
        id: &CheckpointId,
        operator: &str,
        partition: u32,
        data: Bytes,
    ) -> Result<String, CheckpointerError> {
        let path_str = self.paths.snapshot(id, operator, partition);
        self.write_with_digest(&path_str, data).await
    }

    async fn save_delta(
        &self,
        id: &CheckpointId,
        operator: &str,
        partition: u32,
        data: Bytes,
    ) -> Result<String, CheckpointerError> {
        let path_str = self.paths.delta(id, operator, partition);
        self.write_with_digest(&path_str, data).await
    }

    async fn load_artifact(&self, path_str: &str) -> Result<Bytes, CheckpointerError> {
        let path = Path::from(path_str);
        let result = self.store.get_opts(&path, GetOptions::default()).await?;
        let data = result.bytes().await?;
        Ok(data)
    }

    async fn update_latest(&self, id: &CheckpointId) -> Result<(), CheckpointerError> {
        let path_str = self.paths.latest_pointer();
        let path = Path::from(path_str.as_str());
        let payload = PutPayload::from_bytes(Bytes::from(id.to_string_id()));
        Self::put_with_retry(self.store.as_ref(), &path, payload).await
    }

    async fn read_latest(&self) -> Result<Option<CheckpointId>, CheckpointerError> {
        let path_str = self.paths.latest_pointer();
        let path = Path::from(path_str.as_str());
        match self.store.get_opts(&path, GetOptions::default()).await {
            Ok(result) => {
                let data = result.bytes().await?;
                let id_str = std::str::from_utf8(&data).map_err(|e| {
                    object_store::Error::Generic {
                        store: "checkpointer",
                        source: Box::new(e),
                    }
                })?;
                let uuid = uuid::Uuid::parse_str(id_str).map_err(|e| {
                    object_store::Error::Generic {
                        store: "checkpointer",
                        source: Box::new(e),
                    }
                })?;
                Ok(Some(CheckpointId::from_uuid(uuid)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(CheckpointerError::ObjectStore(e)),
        }
    }

    async fn list_checkpoints(&self) -> Result<Vec<CheckpointId>, CheckpointerError> {
        // List objects at the base prefix — each checkpoint is a directory
        // We look for manifest.json files to identify checkpoints
        let prefix = Path::from(self.paths.latest_pointer().trim_end_matches("_latest"));
        let mut ids = Vec::new();

        let mut stream = self.store.list(Some(&prefix));
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            let path_str = meta.location.to_string();
            if path_str.ends_with("manifest.json") {
                // Extract checkpoint ID from path: .../UUID/manifest.json
                if let Some(id_str) = path_str
                    .strip_suffix("/manifest.json")
                    .and_then(|s: &str| s.rsplit('/').next())
                {
                    if let Ok(uuid) = uuid::Uuid::parse_str(id_str) {
                        ids.push(CheckpointId::from_uuid(uuid));
                    }
                }
            }
        }

        ids.sort();
        Ok(ids)
    }

    async fn delete_checkpoint(&self, id: &CheckpointId) -> Result<(), CheckpointerError> {
        // List all objects under this checkpoint's directory and delete them
        let dir = self.paths.checkpoint_dir(id);
        let prefix = Path::from(dir.as_str());

        let mut paths_to_delete = Vec::new();
        let mut stream = self.store.list(Some(&prefix));
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            paths_to_delete.push(meta.location);
        }

        // Delete using delete_stream (object-safe, no ObjectStoreExt needed)
        let locations_stream =
            futures::stream::iter(paths_to_delete.into_iter().map(Ok)).boxed();
        let mut results = self.store.delete_stream(locations_stream);
        while let Some(result) = results.next().await {
            result?;
        }

        Ok(())
    }
}

/// Verify a loaded artifact against its expected SHA-256 digest.
///
/// # Errors
///
/// Returns [`CheckpointerError::IntegrityMismatch`] if the digest doesn't match.
pub fn verify_integrity(
    path: &str,
    data: &[u8],
    expected_sha256: &str,
) -> Result<(), CheckpointerError> {
    let actual = ObjectStoreCheckpointer::sha256_hex(data);
    if actual != expected_sha256 {
        return Err(CheckpointerError::IntegrityMismatch {
            path: path.to_string(),
            expected: expected_sha256.to_string(),
            actual,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::layout::OperatorSnapshotEntry;
    use object_store::memory::InMemory;

    async fn make_checkpointer() -> ObjectStoreCheckpointer {
        let store = Arc::new(InMemory::new());
        let paths = CheckpointPaths::default();
        ObjectStoreCheckpointer::new(store, paths, 4)
    }

    #[tokio::test]
    async fn test_save_and_load_manifest() {
        let ckpt = make_checkpointer().await;
        let id = CheckpointId::now();
        let manifest = CheckpointManifestV2::new(id, 1);

        ckpt.save_manifest(&manifest).await.unwrap();
        let loaded = ckpt.load_manifest(&id).await.unwrap();

        assert_eq!(loaded.checkpoint_id, id);
        assert_eq!(loaded.epoch, 1);
    }

    #[tokio::test]
    async fn test_save_snapshot_with_digest() {
        let ckpt = make_checkpointer().await;
        let id = CheckpointId::now();
        let data = Bytes::from_static(b"hello world");

        let digest = ckpt
            .save_snapshot(&id, "window-agg", 0, data.clone())
            .await
            .unwrap();

        // Verify digest
        assert!(!digest.is_empty());
        verify_integrity("test", &data, &digest).unwrap();
    }

    #[tokio::test]
    async fn test_load_artifact() {
        let ckpt = make_checkpointer().await;
        let id = CheckpointId::now();
        let data = Bytes::from_static(b"partition state");

        ckpt.save_snapshot(&id, "op1", 0, data.clone())
            .await
            .unwrap();

        let path = ckpt.paths.snapshot(&id, "op1", 0);
        let loaded = ckpt.load_artifact(&path).await.unwrap();
        assert_eq!(loaded, data);
    }

    #[tokio::test]
    async fn test_latest_pointer() {
        let ckpt = make_checkpointer().await;

        // No latest yet
        assert!(ckpt.read_latest().await.unwrap().is_none());

        let id = CheckpointId::now();
        ckpt.update_latest(&id).await.unwrap();

        let latest = ckpt.read_latest().await.unwrap().unwrap();
        assert_eq!(latest, id);
    }

    #[tokio::test]
    async fn test_concurrent_partition_uploads() {
        let ckpt = make_checkpointer().await;
        let id = CheckpointId::now();

        let snapshots = vec![
            ("op1".into(), 0, false, Bytes::from_static(b"part0")),
            ("op1".into(), 1, false, Bytes::from_static(b"part1")),
            ("op1".into(), 2, true, Bytes::from_static(b"delta2")),
        ];

        let entries = ckpt
            .save_partitions_concurrent(&id, snapshots)
            .await
            .unwrap();

        assert_eq!(entries.len(), 3);
        for (_, entry) in &entries {
            assert!(entry.sha256.is_some());
        }
    }

    #[tokio::test]
    async fn test_integrity_mismatch() {
        let result = verify_integrity("test.snap", b"data", "wrong_hash");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CheckpointerError::IntegrityMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn test_save_and_build_manifest() {
        let ckpt = make_checkpointer().await;
        let id = CheckpointId::now();

        // Save partitions
        let snapshots = vec![
            ("op1".into(), 0, false, Bytes::from_static(b"state0")),
            ("op1".into(), 1, false, Bytes::from_static(b"state1")),
        ];
        let entries = ckpt
            .save_partitions_concurrent(&id, snapshots)
            .await
            .unwrap();

        // Build manifest from entries
        let mut manifest = CheckpointManifestV2::new(id, 5);
        let mut op_entry = OperatorSnapshotEntry {
            partitions: Vec::new(),
            total_bytes: 0,
        };
        for (_, part) in entries {
            op_entry.total_bytes += part.size_bytes;
            op_entry.partitions.push(part);
        }
        manifest.operators.insert("op1".into(), op_entry);

        ckpt.save_manifest(&manifest).await.unwrap();
        ckpt.update_latest(&id).await.unwrap();

        // Verify round-trip
        let loaded = ckpt.load_manifest(&id).await.unwrap();
        assert_eq!(loaded.operators.len(), 1);
        assert_eq!(loaded.operators["op1"].partitions.len(), 2);
    }
}
