//! [`StateBackendConfig`]: tagged enum selecting storage for checkpointed
//! vnode artifacts. Three shapes: `in_process`, `local` (filesystem path),
//! `object_store` (S3/GCS/Azure/file URL).

use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;

use super::{
    backend::{StateBackend, StateBackendDurability},
    in_process::InProcessBackend,
    object_store::ObjectStoreBackend,
};

/// Default number of vnodes if the user does not override.
pub const DEFAULT_VNODE_CAPACITY: u32 = 256;

/// Largest vnode count representable by the persisted checkpoint ABI.
pub const MAX_VNODE_CAPACITY: u32 = 65_535;

const LOCAL_STATE_WRITER_ID: &str = "local";

fn default_vnode_capacity() -> u32 {
    DEFAULT_VNODE_CAPACITY
}

/// Cloud credential/config overrides for the state object store.
/// `Debug` redacts values — they can hold secrets
/// (`aws_secret_access_key`, ...).
#[derive(Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(transparent)]
pub struct StorageOptions(pub rustc_hash::FxHashMap<String, String>);

impl std::fmt::Debug for StorageOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.0.keys().map(|k| (k, "[REDACTED]")))
            .finish()
    }
}

/// Tagged-union config that selects checkpoint-artifact storage.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
pub enum StateBackendConfig {
    /// Non-durable in-process backend. The default.
    InProcess {
        /// Number of vnodes the backend should size for.
        #[serde(default = "default_vnode_capacity")]
        vnode_capacity: u32,
    },

    /// Durable single-node backend on a local filesystem path. Shorthand
    /// for an `object_store` backend with a `file://` URL.
    Local {
        /// Filesystem root for state.
        path: PathBuf,
        /// Number of vnodes the backend should size for.
        #[serde(default = "default_vnode_capacity")]
        vnode_capacity: u32,
    },

    /// Object-store backend. Cloud URLs (`s3://`, `gs://`, `az://`) are
    /// cluster-shared; `file://` is node-durable only.
    ObjectStore {
        /// Object store URL: `s3://bucket/prefix`, `gs://bucket/prefix`,
        /// etc.
        url: String,
        /// Cloud credentials/config overrides (e.g. `endpoint`,
        /// `aws_access_key_id`), same keys as `[checkpoint.storage]`.
        /// Anything absent falls back to the provider's standard env
        /// vars (`AWS_ACCESS_KEY_ID`, ...).
        #[serde(default)]
        storage: StorageOptions,
        /// Number of vnodes the backend should size for.
        #[serde(default = "default_vnode_capacity")]
        vnode_capacity: u32,
    },
}

impl Default for StateBackendConfig {
    fn default() -> Self {
        Self::InProcess {
            vnode_capacity: DEFAULT_VNODE_CAPACITY,
        }
    }
}

/// Failure modes for [`StateBackendConfig::build`].
#[derive(Debug, thiserror::Error)]
pub enum StateBackendBuildError {
    /// Configuration cannot be represented safely by the runtime/checkpoint ABI.
    #[error("invalid state backend configuration: {0}")]
    InvalidConfig(String),

    /// Object store construction failed (bad URL, missing feature
    /// flag for the scheme, missing credentials, ...).
    #[error("state backend object store: {0}")]
    Store(#[from] crate::checkpoint::object_store_builder::ObjectStoreBuilderError),

    /// Backend construction failed at the I/O layer.
    #[error("state backend construction failed: {0}")]
    Io(String),
}

impl StateBackendConfig {
    /// Builder: embedded library, single process.
    #[must_use]
    pub fn in_process() -> Self {
        Self::InProcess {
            vnode_capacity: DEFAULT_VNODE_CAPACITY,
        }
    }

    /// Builder: single-node durable state on the local filesystem.
    #[must_use]
    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self::Local {
            path: path.into(),
            vnode_capacity: DEFAULT_VNODE_CAPACITY,
        }
    }

    /// Builder: object-store-backed state for an embedded or single-node runtime.
    /// Credentials resolve from the provider's standard env vars; use
    /// the `storage` config field for explicit overrides.
    #[must_use]
    pub fn object_store(url: impl Into<String>) -> Self {
        Self::ObjectStore {
            url: url.into(),
            storage: StorageOptions::default(),
            vnode_capacity: DEFAULT_VNODE_CAPACITY,
        }
    }

    /// Instantiate the runtime backend.
    ///
    /// Declared `async` because backends added in later iterations
    /// (object store, distributed) need to perform async setup. The
    /// in-process path completes synchronously today; callers must
    /// still `.await` for forward-compatibility.
    ///
    /// # Errors
    /// - [`StateBackendBuildError::Store`] for a bad URL, a scheme
    ///   whose feature flag (`aws`/`gcs`/`azure`) is not compiled in,
    ///   or cloud-client construction failure.
    /// - [`StateBackendBuildError::Io`] on filesystem setup.
    #[allow(clippy::unused_async)]
    pub async fn build(&self) -> Result<Arc<dyn StateBackend>, StateBackendBuildError> {
        self.validate()?;
        match self {
            Self::InProcess { vnode_capacity } => {
                Ok(Arc::new(InProcessBackend::new(*vnode_capacity)))
            }
            Self::Local {
                path,
                vnode_capacity,
            } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| StateBackendBuildError::Io(e.to_string()))?;
                let fs = ::object_store::local::LocalFileSystem::new_with_prefix(path)
                    .map_err(|e| StateBackendBuildError::Io(e.to_string()))?;
                Ok(Arc::new(ObjectStoreBackend::node_durable(
                    Arc::new(fs),
                    LOCAL_STATE_WRITER_ID,
                    *vnode_capacity,
                )))
            }
            Self::ObjectStore {
                url,
                storage,
                vnode_capacity,
                ..
            } => {
                let store = cloud_store(url, storage)?;
                let backend = match self.durability_scope() {
                    StateBackendDurability::Volatile => {
                        ObjectStoreBackend::new(store, LOCAL_STATE_WRITER_ID, *vnode_capacity)
                    }
                    StateBackendDurability::NodeDurable => ObjectStoreBackend::node_durable(
                        store,
                        LOCAL_STATE_WRITER_ID,
                        *vnode_capacity,
                    ),
                    StateBackendDurability::ClusterShared => ObjectStoreBackend::cluster_shared(
                        store,
                        LOCAL_STATE_WRITER_ID,
                        *vnode_capacity,
                    ),
                };
                Ok(Arc::new(backend))
            }
        }
    }

    /// Filesystem path for durable state, if any. Returns `None` for
    /// non-filesystem backends.
    #[must_use]
    pub fn local_storage_dir(&self) -> Option<&std::path::Path> {
        match self {
            Self::Local { path, .. } => Some(path.as_path()),
            _ => None,
        }
    }

    /// Build the underlying `object_store` handle (if any) so callers
    /// that need to share the same store — e.g. an
    /// `AssignmentSnapshotStore` alongside the state backend — can
    /// avoid re-parsing the URL. `None` for `InProcess`.
    ///
    /// # Errors
    /// Same failure modes as [`Self::build`].
    pub fn build_object_store(
        &self,
    ) -> Result<Option<Arc<dyn ::object_store::ObjectStore>>, StateBackendBuildError> {
        match self {
            Self::InProcess { .. } => Ok(None),
            Self::Local { path, .. } => {
                std::fs::create_dir_all(path)
                    .map_err(|e| StateBackendBuildError::Io(e.to_string()))?;
                let fs = ::object_store::local::LocalFileSystem::new_with_prefix(path)
                    .map_err(|e| StateBackendBuildError::Io(e.to_string()))?;
                Ok(Some(Arc::new(fs)))
            }
            Self::ObjectStore { url, storage, .. } => Ok(Some(cloud_store(url, storage)?)),
        }
    }

    /// Failure scope survived by the configured backend.
    ///
    /// A local path and `file://` survive a same-node process restart but do
    /// not claim peer visibility. Supported cloud schemes name a shared
    /// service and therefore satisfy cluster recovery admission.
    #[must_use]
    pub fn durability_scope(&self) -> StateBackendDurability {
        match self {
            Self::InProcess { .. } => StateBackendDurability::Volatile,
            Self::Local { .. } => StateBackendDurability::NodeDurable,
            Self::ObjectStore { url, .. } => StateBackendDurability::for_storage_url(url),
        }
    }

    /// Number of vnodes this backend is sized for.
    #[must_use]
    pub fn vnode_capacity(&self) -> u32 {
        match self {
            Self::InProcess { vnode_capacity }
            | Self::Local { vnode_capacity, .. }
            | Self::ObjectStore { vnode_capacity, .. } => *vnode_capacity,
        }
    }

    /// Validate invariants shared by every runtime mode.
    ///
    /// # Errors
    /// Returns [`StateBackendBuildError::InvalidConfig`] when the vnode count cannot be encoded
    /// by the persisted checkpoint format.
    pub fn validate(&self) -> Result<(), StateBackendBuildError> {
        let capacity = self.vnode_capacity();
        if !(1..=MAX_VNODE_CAPACITY).contains(&capacity) {
            return Err(StateBackendBuildError::InvalidConfig(format!(
                "vnode_capacity must be between 1 and {MAX_VNODE_CAPACITY}, got {capacity}"
            )));
        }
        Ok(())
    }
}

/// Cloud-store construction shared by [`StateBackendConfig::build`] and
/// [`StateBackendConfig::build_object_store`]: translates the
/// `StorageOptions` map into the builder's std-HashMap parameter
/// (cold path, runs once at startup).
fn cloud_store(
    url: &str,
    storage: &StorageOptions,
) -> Result<Arc<dyn ::object_store::ObjectStore>, StateBackendBuildError> {
    Ok(crate::checkpoint::object_store_builder::build_object_store(
        url,
        &storage
            .0
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn persisted_seal_writer(
        root: &std::path::Path,
        attempt: crate::state::CheckpointAttempt,
    ) -> String {
        let path = root
            .join("state-v2")
            .join(format!("epoch={}", attempt.epoch))
            .join(format!("checkpoint={}", attempt.checkpoint_id))
            .join("_SEAL");
        let bytes = std::fs::read(path).unwrap();
        let seal: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        seal["instance_id"].as_str().unwrap().to_owned()
    }

    #[test]
    fn parse_in_process_minimal() {
        let toml = r#"backend = "in_process""#;
        let c: StateBackendConfig = toml::from_str(toml).unwrap();
        assert!(matches!(
            c,
            StateBackendConfig::InProcess {
                vnode_capacity: 256
            }
        ));
        assert_eq!(c.durability_scope(), StateBackendDurability::Volatile);
        assert!(c.local_storage_dir().is_none());
    }

    #[test]
    fn parse_local_with_path() {
        let toml = r#"
backend = "local"
path = "/var/laminar"
vnode_capacity = 128
"#;
        let c: StateBackendConfig = toml::from_str(toml).unwrap();
        assert_eq!(
            c.local_storage_dir(),
            Some(std::path::Path::new("/var/laminar"))
        );
        assert_eq!(c.durability_scope(), StateBackendDurability::NodeDurable);
        if let StateBackendConfig::Local { vnode_capacity, .. } = c {
            assert_eq!(vnode_capacity, 128);
        } else {
            panic!("expected Local");
        }
    }

    #[test]
    fn storage_url_durability_is_fail_closed() {
        assert_eq!(
            StateBackendDurability::for_storage_url("file:///var/lib/laminar"),
            StateBackendDurability::NodeDurable
        );
        assert_eq!(
            StateBackendDurability::for_storage_url("s3://bucket/state"),
            StateBackendDurability::ClusterShared
        );
        assert_eq!(
            StateBackendDurability::for_storage_url("memory://state"),
            StateBackendDurability::Volatile
        );
        assert_eq!(
            StateBackendDurability::for_storage_url("custom://state"),
            StateBackendDurability::Volatile
        );
    }

    #[test]
    fn parse_object_store() {
        let toml = r#"
backend = "object_store"
url = "s3://bucket/laminar"
"#;
        let c: StateBackendConfig = toml::from_str(toml).unwrap();
        assert_eq!(c.durability_scope(), StateBackendDurability::ClusterShared);
        match c {
            StateBackendConfig::ObjectStore { url, .. } => {
                assert_eq!(url, "s3://bucket/laminar");
            }
            _ => panic!("expected ObjectStore"),
        }
    }

    #[test]
    fn reject_public_state_writer_identity() {
        for toml in [
            "backend = \"local\"\npath = \"/var/laminar\"\ninstance_id = \"node-0\"\n",
            "backend = \"local\"\npath = \"/var/laminar\"\ninstanceId = \"node-0\"\n",
            "backend = \"object_store\"\nurl = \"s3://bucket/laminar\"\ninstance_id = \"node-0\"\n",
            "backend = \"object_store\"\nurl = \"s3://bucket/laminar\"\ninstanceId = \"node-0\"\n",
        ] {
            assert!(
                toml::from_str::<StateBackendConfig>(toml).is_err(),
                "public writer identity was silently accepted: {toml}"
            );
        }
    }

    #[test]
    fn reject_unwired_object_store_fields() {
        for retired in [
            "vnodes = [0, 1]",
            "merger_instance = \"node-0\"",
            "discovery = \"dynamic\"",
            "seed_peers = [\"10.0.0.1:7946\"]",
        ] {
            let toml =
                format!("backend = \"object_store\"\nurl = \"s3://bucket/laminar\"\n{retired}\n");
            assert!(
                toml::from_str::<StateBackendConfig>(&toml).is_err(),
                "retired field was silently accepted: {retired}"
            );
        }
    }

    #[tokio::test]
    async fn build_in_process_returns_backend() {
        use crate::state::CheckpointAttempt;
        use bytes::Bytes;
        let c = StateBackendConfig::in_process();
        let backend = c.build().await.unwrap();
        assert_eq!(backend.durability_scope(), StateBackendDurability::Volatile);
        let attempt = CheckpointAttempt::new(1, 1);
        backend
            .write_partial(attempt, 0, 0, Bytes::from_static(b"ok"))
            .await
            .unwrap();
        assert_eq!(
            &backend.read_partial(attempt, 0).await.unwrap().unwrap()[..],
            b"ok",
        );
    }

    #[tokio::test]
    async fn build_local_instantiates_backend() {
        use crate::state::CheckpointAttempt;
        let dir = tempfile::tempdir().unwrap();
        let c = StateBackendConfig::local(dir.path());
        let backend = c.build().await.unwrap();
        assert_eq!(
            backend.durability_scope(),
            StateBackendDurability::NodeDurable
        );
        let attempt = CheckpointAttempt::new(1, 1);
        backend
            .write_partial(attempt, 0, 0, bytes::Bytes::from_static(b"z"))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(attempt, None, &[0], &[])
            .await
            .unwrap());
        assert_eq!(persisted_seal_writer(dir.path(), attempt), "local");
        assert_eq!(
            &backend.read_partial(attempt, 0).await.unwrap().unwrap()[..],
            b"z",
        );
    }

    #[tokio::test]
    async fn build_object_store_file_url_instantiates_backend() {
        use crate::state::CheckpointAttempt;
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "file://{}",
            dir.path().display().to_string().replace('\\', "/")
        );
        let c = StateBackendConfig::object_store(url);
        let backend = c.build().await.unwrap();
        assert_eq!(
            backend.durability_scope(),
            StateBackendDurability::NodeDurable
        );
        let attempt = CheckpointAttempt::new(1, 1);
        backend
            .write_partial(attempt, 0, 0, bytes::Bytes::from_static(b"z"))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(attempt, None, &[0], &[])
            .await
            .unwrap());
        assert_eq!(persisted_seal_writer(dir.path(), attempt), "local");
        let got = backend.read_partial(attempt, 0).await.unwrap().unwrap();
        assert_eq!(&got[..], b"z");
    }

    /// Without the `aws` feature an `s3://` URL must fail with the
    /// missing-feature error, not silently fall back to local.
    #[cfg(not(feature = "aws"))]
    #[tokio::test]
    async fn build_object_store_s3_requires_aws_feature() {
        use crate::checkpoint::object_store_builder::ObjectStoreBuilderError;

        let c = StateBackendConfig::object_store("s3://bucket/path");
        let Err(err) = c.build().await else {
            panic!("s3 must not build without the aws feature");
        };
        assert!(
            matches!(
                err,
                StateBackendBuildError::Store(ObjectStoreBuilderError::MissingFeature { .. })
            ),
            "got: {err}",
        );
    }

    /// With the `aws` feature, an `s3://` URL + explicit `storage`
    /// credentials builds a client (construction is offline — no
    /// network until first use).
    #[cfg(feature = "aws")]
    #[tokio::test]
    async fn build_object_store_s3_builds_with_storage_options() {
        let toml = r#"
backend = "object_store"
url = "s3://bucket/laminar"

[storage]
endpoint = "http://127.0.0.1:9000"
aws_access_key_id = "k"
aws_secret_access_key = "s"
region = "us-east-1"
allow_http = "true"
"#;
        let c: StateBackendConfig = toml::from_str(toml).unwrap();
        c.build().await.expect("s3 client must build offline");
    }

    #[test]
    fn default_is_in_process() {
        let c = StateBackendConfig::default();
        assert!(matches!(c, StateBackendConfig::InProcess { .. }));
    }

    #[test]
    fn partial_eq_works() {
        assert_eq!(
            StateBackendConfig::in_process(),
            StateBackendConfig::in_process()
        );
        assert_ne!(
            StateBackendConfig::in_process(),
            StateBackendConfig::local("/tmp/x")
        );
    }

    #[tokio::test]
    async fn build_rejects_vnode_capacity_outside_persisted_range() {
        for vnode_capacity in [0, MAX_VNODE_CAPACITY + 1] {
            let config = StateBackendConfig::InProcess { vnode_capacity };
            let Err(error) = config.build().await else {
                panic!("out-of-range vnode capacity was accepted");
            };
            assert!(matches!(error, StateBackendBuildError::InvalidConfig(_)));
        }
    }
}
