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
    KeyGroupCount,
};

const LOCAL_STATE_WRITER_ID: &str = "local";
const LOCAL_STATE_LOCK: &str = ".laminardb-state.lock";

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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StateBackendConfig {
    /// Non-durable in-process backend. The default.
    #[default]
    InProcess,

    /// Durable single-node backend on a local filesystem path. Shorthand
    /// for an `object_store` backend with a `file://` URL.
    Local {
        /// Filesystem root for state.
        path: PathBuf,
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
        storage: StorageOptions,
    },
}

#[derive(Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
enum StrictStateBackendConfig {
    InProcess {},
    Local {
        path: PathBuf,
    },
    ObjectStore {
        url: String,
        #[serde(default)]
        storage: StorageOptions,
    },
}

impl<'de> Deserialize<'de> for StateBackendConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match StrictStateBackendConfig::deserialize(deserializer)? {
            StrictStateBackendConfig::InProcess {} => Self::InProcess,
            StrictStateBackendConfig::Local { path } => Self::Local { path },
            StrictStateBackendConfig::ObjectStore { url, storage } => {
                Self::ObjectStore { url, storage }
            }
        })
    }
}

/// Failure modes for [`StateBackendConfig::build`].
#[derive(Debug, thiserror::Error)]
pub enum StateBackendBuildError {
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
        Self::InProcess
    }

    /// Builder: single-node durable state on the local filesystem.
    #[must_use]
    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self::Local { path: path.into() }
    }

    /// Builder: object-store-backed state for an embedded or single-node runtime.
    /// Credentials resolve from the provider's standard env vars; use
    /// the `storage` config field for explicit overrides.
    #[must_use]
    pub fn object_store(url: impl Into<String>) -> Self {
        Self::ObjectStore {
            url: url.into(),
            storage: StorageOptions::default(),
        }
    }

    /// Instantiate the runtime backend.
    ///
    /// # Errors
    /// - [`StateBackendBuildError::Store`] for a bad URL, a scheme
    ///   whose feature flag (`aws`/`gcs`/`azure`) is not compiled in,
    ///   or cloud-client construction failure.
    /// - [`StateBackendBuildError::Io`] on filesystem setup.
    pub fn build(
        &self,
        key_groups: KeyGroupCount,
    ) -> Result<Arc<dyn StateBackend>, StateBackendBuildError> {
        let key_group_count = u32::from(key_groups);
        match self {
            Self::InProcess => Ok(Arc::new(InProcessBackend::new(key_group_count))),
            Self::Local { path } => {
                let fs = durable_local_store(path)?;
                Ok(Arc::new(
                    ObjectStoreBackend::node_durable_with_empty_prefix_cleanup(
                        fs,
                        LOCAL_STATE_WRITER_ID,
                        key_group_count,
                    ),
                ))
            }
            Self::ObjectStore { url, .. } if url.starts_with("file://") => {
                let path = crate::checkpoint::object_store_builder::file_url_path(url)?;
                let fs = durable_local_store(&path)?;
                Ok(Arc::new(
                    ObjectStoreBackend::node_durable_with_empty_prefix_cleanup(
                        fs,
                        LOCAL_STATE_WRITER_ID,
                        key_group_count,
                    ),
                ))
            }
            Self::ObjectStore { url, storage } => {
                let store = cloud_store(url, storage)?;
                let backend = match self.durability_scope() {
                    StateBackendDurability::Volatile => {
                        ObjectStoreBackend::new(store, LOCAL_STATE_WRITER_ID, key_group_count)
                    }
                    StateBackendDurability::NodeDurable => ObjectStoreBackend::node_durable(
                        store,
                        LOCAL_STATE_WRITER_ID,
                        key_group_count,
                    ),
                    StateBackendDurability::ClusterShared => ObjectStoreBackend::cluster_shared(
                        store,
                        LOCAL_STATE_WRITER_ID,
                        key_group_count,
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
            Self::InProcess => Ok(None),
            Self::Local { path } => Ok(Some(durable_local_store(path)?)),
            Self::ObjectStore { url, storage, .. } => Ok(Some(state_store(url, storage)?)),
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
            Self::InProcess => StateBackendDurability::Volatile,
            Self::Local { .. } => StateBackendDurability::NodeDurable,
            Self::ObjectStore { url, .. } => StateBackendDurability::for_storage_url(url),
        }
    }
}

fn durable_local_store(
    path: &std::path::Path,
) -> Result<Arc<crate::durable_local_store::DurableLocalObjectStore>, StateBackendBuildError> {
    crate::durable_local_store::DurableLocalObjectStore::new_exclusive(path, LOCAL_STATE_LOCK)
        .map(Arc::new)
        .map_err(|error| StateBackendBuildError::Io(error.to_string()))
}

fn state_store(
    url: &str,
    storage: &StorageOptions,
) -> Result<Arc<dyn ::object_store::ObjectStore>, StateBackendBuildError> {
    if url.starts_with("file://") {
        let path = crate::checkpoint::object_store_builder::file_url_path(url)?;
        durable_local_store(&path).map(|store| store as Arc<dyn ::object_store::ObjectStore>)
    } else {
        cloud_store(url, storage)
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
        assert!(matches!(c, StateBackendConfig::InProcess));
        assert_eq!(c.durability_scope(), StateBackendDurability::Volatile);
        assert!(c.local_storage_dir().is_none());
    }

    #[test]
    fn parse_local_with_path() {
        let toml = r#"
backend = "local"
path = "/var/laminar"
"#;
        let c: StateBackendConfig = toml::from_str(toml).unwrap();
        assert_eq!(
            c.local_storage_dir(),
            Some(std::path::Path::new("/var/laminar"))
        );
        assert_eq!(c.durability_scope(), StateBackendDurability::NodeDurable);
        assert!(matches!(c, StateBackendConfig::Local { .. }));
    }

    #[test]
    fn storage_url_durability_is_fail_closed() {
        let local_url = url::Url::from_file_path(std::env::temp_dir().join("laminar-state"))
            .unwrap()
            .to_string();
        assert_eq!(
            StateBackendDurability::for_storage_url(&local_url),
            StateBackendDurability::NodeDurable
        );
        assert_eq!(
            StateBackendDurability::for_storage_url("file:///tmp/laminar-state"),
            StateBackendDurability::NodeDurable
        );
        assert_eq!(
            StateBackendDurability::for_storage_url("file://remote-host/state"),
            StateBackendDurability::Volatile
        );
        assert_eq!(
            StateBackendDurability::for_storage_url("file://./relative"),
            StateBackendDurability::Volatile
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

    #[test]
    fn reject_backend_specific_vnode_capacity() {
        for toml in [
            "backend = \"in_process\"\nvnode_capacity = 256\n",
            "backend = \"local\"\npath = \"/var/laminar\"\nvnode_capacity = 256\n",
            "backend = \"object_store\"\nurl = \"s3://bucket/state\"\nvnode_capacity = 256\n",
        ] {
            assert!(toml::from_str::<StateBackendConfig>(toml).is_err());
        }
    }

    #[tokio::test]
    async fn build_in_process_returns_backend() {
        use crate::state::CheckpointAttempt;
        use bytes::Bytes;
        let c = StateBackendConfig::in_process();
        let backend = c.build(super::super::LOCAL_KEY_GROUP_COUNT).unwrap();
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
        let backend = c.build(super::super::LOCAL_KEY_GROUP_COUNT).unwrap();
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
        drop(backend);
        let restarted = c.build(super::super::LOCAL_KEY_GROUP_COUNT).unwrap();
        assert_eq!(
            &restarted.read_partial(attempt, 0).await.unwrap().unwrap()[..],
            b"z",
        );
    }

    #[test]
    fn local_state_root_has_one_live_owner() {
        let dir = tempfile::tempdir().unwrap();
        let config = StateBackendConfig::local(dir.path());
        let first = config.build(super::super::LOCAL_KEY_GROUP_COUNT).unwrap();
        let Err(error) = config.build(super::super::LOCAL_KEY_GROUP_COUNT) else {
            panic!("a second local state owner was admitted");
        };
        assert!(error.to_string().contains("already owned"));
        drop(first);
        config.build(super::super::LOCAL_KEY_GROUP_COUNT).unwrap();
    }

    #[tokio::test]
    async fn build_object_store_file_url_instantiates_backend() {
        use crate::state::CheckpointAttempt;
        let dir = tempfile::tempdir().unwrap();
        let normalized = dir.path().display().to_string().replace('\\', "/");
        let url = if normalized.starts_with('/') {
            format!("file://{normalized}")
        } else {
            format!("file:///{normalized}")
        };
        let c = StateBackendConfig::object_store(url);
        let backend = c.build(super::super::LOCAL_KEY_GROUP_COUNT).unwrap();
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
    #[test]
    fn build_object_store_s3_requires_aws_feature() {
        use crate::checkpoint::object_store_builder::ObjectStoreBuilderError;

        let c = StateBackendConfig::object_store("s3://bucket/path");
        let Err(err) = c.build(super::super::LOCAL_KEY_GROUP_COUNT) else {
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
    #[test]
    fn build_object_store_s3_builds_with_storage_options() {
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
        c.build(super::super::LOCAL_KEY_GROUP_COUNT)
            .expect("s3 client must build offline");
    }

    #[test]
    fn default_is_in_process() {
        let c = StateBackendConfig::default();
        assert!(matches!(c, StateBackendConfig::InProcess));
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
}
