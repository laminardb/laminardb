//! Cluster-wide catalog manifest: the ordered DDL needed to rebuild a
//! node's logical catalog (sources, tables, streams, materialized views,
//! sinks).
//!
//! Unlike [`AssignmentSnapshot`](super::AssignmentSnapshot) (one immutable
//! object per version, CAS-rotated), the catalog manifest is a single
//! mutable object at `catalog/manifest.json` that the cluster overwrites as
//! DDL changes. A node joining or restarting reads it and replays any DDL it
//! lacks locally so its operator graph exists before the pipeline starts —
//! the data sharded across vnodes then flows into the rebuilt views.
//!
//! Entries are stored in creation order so a dependent view (one selecting
//! from another) is replayed after its dependency.

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

const MANIFEST_PATH: &str = "catalog/manifest.json";

/// One catalog object's defining DDL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogManifestEntry {
    /// Object name (source/sink/stream/view/table identifier).
    pub name: String,
    /// The exact DDL text that created it, replayed verbatim on restore.
    pub ddl: String,
}

/// The full ordered set of DDL defining a cluster's catalog.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogManifest {
    /// Monotonic version, bumped on each save. Diagnostic only — the
    /// manifest is overwritten in place, not CAS-rotated.
    pub version: u64,
    /// Wall-clock of the last save, millis since epoch.
    pub updated_at_ms: i64,
    /// DDL entries in creation (dependency-safe) order.
    pub entries: Vec<CatalogManifestEntry>,
}

/// I/O wrapper for the [`CatalogManifest`] on an object store.
pub struct CatalogManifestStore {
    store: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for CatalogManifestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogManifestStore")
            .finish_non_exhaustive()
    }
}

/// Errors loading or saving a [`CatalogManifest`].
#[derive(Debug, thiserror::Error)]
pub enum CatalogManifestError {
    /// Underlying object store I/O failure.
    #[error("object store I/O: {0}")]
    Io(String),
    /// JSON de/serialization failure.
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
}

impl CatalogManifestStore {
    /// Wrap a pre-constructed object store.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    fn path() -> OsPath {
        OsPath::from(MANIFEST_PATH)
    }

    /// Load the current catalog manifest; `Ok(None)` on a fresh cluster.
    ///
    /// # Errors
    /// Object-store I/O or JSON decode failure.
    pub async fn load(&self) -> Result<Option<CatalogManifest>, CatalogManifestError> {
        match self.store.get(&Self::path()).await {
            Ok(res) => {
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|e| CatalogManifestError::Io(e.to_string()))?;
                Ok(Some(serde_json::from_slice(&bytes)?))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(CatalogManifestError::Io(e.to_string())),
        }
    }

    /// Overwrite the catalog manifest. Last writer wins — the leader (or any
    /// node executing catalog DDL) publishes the latest full catalog.
    ///
    /// # Errors
    /// Object-store I/O or JSON encode failure.
    pub async fn save(&self, manifest: &CatalogManifest) -> Result<(), CatalogManifestError> {
        let bytes = serde_json::to_vec_pretty(manifest)?;
        self.store
            .put(&Self::path(), PutPayload::from(Bytes::from(bytes)))
            .await
            .map_err(|e| CatalogManifestError::Io(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;

    fn store_in(dir: &std::path::Path) -> CatalogManifestStore {
        let fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
        CatalogManifestStore::new(fs)
    }

    fn entry(name: &str, ddl: &str) -> CatalogManifestEntry {
        CatalogManifestEntry {
            name: name.to_string(),
            ddl: ddl.to_string(),
        }
    }

    #[tokio::test]
    async fn load_missing_returns_none() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        assert!(s.load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn save_then_load_preserves_order() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        let manifest = CatalogManifest {
            version: 3,
            updated_at_ms: 123,
            entries: vec![
                entry("src", "CREATE SOURCE src (k BIGINT)"),
                entry("mv1", "CREATE MATERIALIZED VIEW mv1 AS SELECT k FROM src"),
                entry("mv2", "CREATE MATERIALIZED VIEW mv2 AS SELECT k FROM mv1"),
            ],
        };
        s.save(&manifest).await.unwrap();
        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded, manifest);
        // Order is dependency-safe: src before mv1 before mv2.
        assert_eq!(loaded.entries[0].name, "src");
        assert_eq!(loaded.entries[2].name, "mv2");
    }

    #[tokio::test]
    async fn save_overwrites_in_place() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        s.save(&CatalogManifest {
            version: 1,
            updated_at_ms: 1,
            entries: vec![entry("a", "CREATE SOURCE a (k BIGINT)")],
        })
        .await
        .unwrap();
        let v2 = CatalogManifest {
            version: 2,
            updated_at_ms: 2,
            entries: vec![
                entry("a", "CREATE SOURCE a (k BIGINT)"),
                entry("b", "CREATE SOURCE b (k BIGINT)"),
            ],
        };
        s.save(&v2).await.unwrap();
        assert_eq!(s.load().await.unwrap().unwrap(), v2);
    }
}
